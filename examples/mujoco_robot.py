#!/usr/bin/env python3
"""
A simulated quadruped to teleoperate through the bridge.

A Unitree Go2 walks in MuJoCo, with its front camera and its lidar. It logs to Rerun like any
robot would, with the stock SDK, and knows nothing about LiveKit: it follows the commands that
`rerun-livekit robot --commands-to-udp` hands it.

MuJoCo Playground trained the policy for the Unitree Go1, which has the same joint layout.

A command is what `keyboard_teleop` sends: a forward speed and a speed to the left in m/s and a
turn rate in rad/s, as three little-endian `f32`. An empty datagram means stop.

One run is one episode. It ends when the robot falls over or the program is interrupted.

Run it with `pixi run mujoco-robot`. On Linux without a display, set `MUJOCO_GL=egl`.
"""

from __future__ import annotations

import argparse
import re
import socket
import struct
import time
import urllib.request
from pathlib import Path

import mujoco
import numpy as np
import onnxruntime as ort
import rerun as rr
import rerun.blueprint as rrb
from rerun.mjcf import MjcfTree

GO2_DIR = Path(__file__).parent / "go2"

MENAGERIE_URL = "https://raw.githubusercontent.com/google-deepmind/mujoco_menagerie/370c43965323c0944b433b202aac5058b5f907a9/unitree_go2"
POLICY_URL = "https://raw.githubusercontent.com/google-deepmind/mujoco_playground/e0842145be50291c4c47c7ffc0424e32d5fdd6eb/rss2025/mujoco_playground/experimental/sim2sim/onnx/go1_policy.onnx"
SCENE_XML = GO2_DIR / "scene_mjx.xml"
ROBOT_XML = GO2_DIR / "go2_mjx.xml"
POLICY_ONNX = GO2_DIR / "go1_policy.onnx"

# The body with the floating base, which the camera and the lidar are mounted on.
BASE_BODY = "base"
# Where Unitree's URDF of the Go2 puts the camera and the lidar, which the MJCF model leaves out:
# https://github.com/unitreerobotics/unitree_ros/blob/master/robots/go2_description/urdf/go2_description.urdf
CAMERA_FRAME = "front_camera"
CAMERA_POSITION = (0.32715, -0.00003, 0.04297)
LIDAR_POSITION = (0.28945, 0.0, -0.046825)
# The lidar hangs upside down under the head, tilted ahead: a rotation around Y, in radians.
LIDAR_PITCH = 2.8782
LIDAR_ROTATION = np.array([
    [np.cos(LIDAR_PITCH), 0.0, np.sin(LIDAR_PITCH)],
    [0.0, 1.0, 0.0],
    [-np.sin(LIDAR_PITCH), 0.0, np.cos(LIDAR_PITCH)],
])

# The joints in the order of the policy's observation and action, and the pose its actions are relative to.
POLICY_JOINTS = [f"{leg}_{joint}" for leg in ("FR", "FL", "RR", "RL") for joint in ("hip", "thigh", "calf")]
POLICY_DEFAULT_ANGLES = np.array([0.1, 0.9, -1.8, -0.1, 0.9, -1.8, 0.1, 0.9, -1.8, -0.1, 0.9, -1.8])

CONTROL_DT = 0.02
ACTION_SCALE = 0.5
FALLEN_UP_Z = 0.3  # The robot has fallen once its up axis tilts more than ~70° away from vertical.

# The command goes back to zero when datagrams stop arriving for this long.
COMMAND_TIMEOUT_SECS = 0.3
COMMAND_FORMAT = "<3f"

# Where each part of a command is logged next to what the robot measured, in the order of a
# command: the speed ahead and to the left in m/s, and the turn rate in rad/s.
VELOCITY_PATHS = ("velocity", "lateral_velocity", "turn_rate")

# The front camera of the Go2, after Unitree's developer documentation. It has no lens distortion here.
# https://doc-cdn.unitree.com/6/53/en/6_53_en
CAMERA_NAME = "front"
CAMERA_WIDTH = 1280
CAMERA_HEIGHT = 720
CAMERA_FOV_X = np.radians(100.0)
CAMERA_FPS = 15.0
# The axes of a MuJoCo camera as columns, in the frame of `BASE_BODY`: it looks along its -Z axis,
# with X to the right and Y up.
CAMERA_ROTATION = np.array([[0.0, 0.0, -1.0], [-1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])

# The Unitree L2 lidar of the Go2, after its manual. A reflector spins at 216 Hz and sweeps the beam
# over the lidar from one side to the other, in a plane that turns around the lidar's Z axis at 5.55 Hz.
# https://oss-global-cdn.unitree.com/static/Unitree%204D%20LiDAR%20L2%20User%20Manual.pdf
LIDAR_REFLECTOR_HZ = 216.0
LIDAR_AZIMUTH_HZ = 5.55
# About half of the samples are effective: for the rest the beam points below the field of view.
LIDAR_SAMPLES_PER_SEC = 128_000
# How far from its Z axis the lidar sees on the Go2, which is 6° below its own horizon.
LIDAR_FOV_FROM_Z = np.radians(96.0)
LIDAR_MIN_RANGE_METERS = 0.05
LIDAR_MAX_RANGE_METERS = 30.0
# The floor and the obstacles are in geom group 0. The robot's own geoms are not, which keeps
# its legs out of the cloud.
LIDAR_GEOM_GROUPS = np.array([1, 0, 0, 0, 0, 0], dtype=np.uint8)
# The Go2 publishes about 15 point clouds a second, which is one every 14 turns of the reflector.
LIDAR_SCANS_PER_CLOUD = 14
# The colours of the points, from the floor up to `LIDAR_COLORS_TOP_METERS`.
LIDAR_COLORS = np.array([[70, 90, 230], [60, 200, 200], [120, 220, 90], [250, 220, 70], [250, 110, 60]])
LIDAR_COLORS_TOP_METERS = 1.0
# How much of the lidar's past the Viewer shows at once.
LIDAR_VISIBLE_SECS = 3.0

# What the robot walks among: the walls of a room with a doorway, pillars and blocks.
# Each is a box, given as its centre (x, y), its half sizes (x, y, z) and its colour.
WALL = (200, 200, 205)
OBSTACLES = [
    ((0.0, 6.0), (6.0, 0.05, 0.5), WALL),
    ((0.0, -6.0), (6.0, 0.05, 0.5), WALL),
    ((6.0, 0.0), (0.05, 6.0, 0.5), WALL),
    ((-6.0, 0.0), (0.05, 6.0, 0.5), WALL),
    # A wall across the room, with a doorway straight ahead of where the robot starts.
    ((2.5, 3.5), (0.05, 2.5, 0.5), WALL),
    ((2.5, -3.5), (0.05, 2.5, 0.5), WALL),
    ((4.5, 0.0), (0.3, 0.3, 0.3), (220, 60, 60)),
    ((4.0, 3.5), (0.5, 0.3, 0.2), (60, 200, 90)),
    ((4.2, -3.0), (0.15, 0.15, 0.6), (230, 200, 60)),
    ((0.5, 3.0), (0.15, 0.15, 0.6), (70, 110, 230)),
    ((-0.5, -3.0), (0.4, 0.4, 0.15), (200, 80, 220)),
    ((-3.0, 2.0), (0.3, 0.6, 0.4), (240, 140, 50)),
    ((-3.5, -2.5), (0.15, 0.15, 0.6), (60, 190, 200)),
]


def download(url: str, path: Path) -> None:
    if path.exists():
        return
    print(f"Downloading {path.name}…")
    path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=60) as response:
        path.write_bytes(response.read())


def download_model() -> None:
    """Downloads the Go2 model from MuJoCo Menagerie."""
    for xml in (SCENE_XML, ROBOT_XML):
        download(f"{MENAGERIE_URL}/{xml.name}", xml)
    for mesh in sorted(set(re.findall(r'<mesh file="(\w+\.obj)"', ROBOT_XML.read_text(encoding="utf-8")))):
        download(f"{MENAGERIE_URL}/assets/{mesh}", GO2_DIR / "assets" / mesh)


def lidar_rays(cloud: int) -> np.ndarray:
    """The unit vectors along the rays of the lidar's `cloud`-th point cloud, in its own frame, one row per ray."""
    samples_per_cloud = round(LIDAR_SAMPLES_PER_SEC * LIDAR_SCANS_PER_CLOUD / LIDAR_REFLECTOR_HZ)
    sample_secs = (cloud * samples_per_cloud + np.arange(samples_per_cloud) + 0.5) / LIDAR_SAMPLES_PER_SEC
    from_z_axis = 2.0 * np.pi * LIDAR_REFLECTOR_HZ * sample_secs
    azimuth = 2.0 * np.pi * LIDAR_AZIMUTH_HZ * sample_secs
    rays = np.column_stack([
        np.sin(from_z_axis) * np.cos(azimuth),
        np.sin(from_z_axis) * np.sin(azimuth),
        np.cos(from_z_axis),
    ])
    return rays[np.cos(LIDAR_FOV_FROM_Z) < rays[:, 2]]


def lidar_cloud(model: mujoco.MjModel, data: mujoco.MjData, cloud: int) -> rr.Points3D:
    """The lidar's `cloud`-th point cloud: where its rays hit the scene, in the world, coloured by height."""
    rays = lidar_rays(cloud)
    hit_geoms = np.zeros(len(rays), dtype=np.int32)
    distances = np.zeros(len(rays))

    base = model.body(BASE_BODY).id
    world_from_base = data.xmat[base].reshape(3, 3)
    origin = data.xpos[base] + world_from_base @ LIDAR_POSITION
    rays_in_world = rays @ (world_from_base @ LIDAR_ROTATION).T
    mujoco.mj_multiRay(
        model,
        data,
        pnt=origin,
        vec=rays_in_world.ravel(),
        geomgroup=LIDAR_GEOM_GROUPS,
        flg_static=True,
        bodyexclude=-1,
        geomid=hit_geoms,
        dist=distances,
        normal=None,
        nray=len(rays),
        cutoff=LIDAR_MAX_RANGE_METERS,
    )
    is_hit = (hit_geoms != -1) & (LIDAR_MIN_RANGE_METERS <= distances)
    points = origin + rays_in_world[is_hit] * distances[is_hit, None]

    stops = np.linspace(0.0, LIDAR_COLORS_TOP_METERS, len(LIDAR_COLORS))
    colors = np.column_stack([np.interp(points[:, 2], stops, channel) for channel in LIDAR_COLORS.T])
    return rr.Points3D(points, colors=colors.astype(np.uint8))


def load_model() -> mujoco.MjModel:
    """The Go2 scene, with its front camera and the obstacles added."""
    spec = mujoco.MjSpec.from_file(str(SCENE_XML))
    # MuJoCo renders offscreen at up to 640x480 unless the model asks for more.
    spec.visual.global_.offwidth = CAMERA_WIDTH
    spec.visual.global_.offheight = CAMERA_HEIGHT

    quat_wxyz = np.zeros(4)
    mujoco.mju_mat2Quat(quat_wxyz, CAMERA_ROTATION.flatten())
    fov_y = 2.0 * np.arctan(np.tan(CAMERA_FOV_X / 2) * CAMERA_HEIGHT / CAMERA_WIDTH)
    spec.body(BASE_BODY).add_camera(name=CAMERA_NAME, pos=CAMERA_POSITION, quat=quat_wxyz, fovy=np.degrees(fov_y))

    for (x, y), half_size, color in OBSTACLES:
        spec.worldbody.add_geom(
            type=mujoco.mjtGeom.mjGEOM_BOX,
            size=half_size,
            pos=[x, y, half_size[2]],
            rgba=[*(channel / 255 for channel in color), 1.0],
        )

    return spec.compile()


def robot_view(mjcf: MjcfTree, exclude: tuple[str, ...] = ()) -> rrb.Spatial3DView:
    """A 3D view that follows the robot, with the lidar's recent points, without the entities in `exclude`."""
    return rrb.Spatial3DView(
        name="Go2",
        # MJCF does not mark geoms as collision geometry, so the importer goes by their group.
        contents=[
            "+ /**",
            f"- /{rr.escape_entity_path_part(mjcf.name)}/collision_geometries/**",
            *(f"- {path}" for path in exclude),
        ],
        eye_controls=rrb.EyeControls3D(
            position=[-3.0, -2.4, 2.4],
            look_target=[0.0, 0.0, 0.3],
            tracking_entity=mjcf.get_visual_geometry_paths(BASE_BODY)[0],
        ),
        overrides={
            "lidar": rrb.VisibleTimeRanges(
                timeline="sim_time",
                start=rrb.TimeRangeBoundary.cursor_relative(seconds=-LIDAR_VISIBLE_SECS),
                end=rrb.TimeRangeBoundary.cursor_relative(),
            )
        },
    )


def blueprint(mjcf: MjcfTree) -> rrb.Blueprint:
    """What the robot sees, next to a 3D view that follows it and how well it follows the commands."""
    return rrb.Blueprint(
        rrb.Horizontal(
            rrb.Spatial2DView(name="Front camera", origin="/camera"),
            rrb.Vertical(
                robot_view(mjcf),
                rrb.TimeSeriesView(name="Forward velocity", origin="/velocity"),
                rrb.TimeSeriesView(name="Turn rate", origin="/turn_rate"),
                row_shares=[3, 1, 1],
            ),
            column_shares=[3, 2],
        ),
        collapse_panels=True,
    )


def log_scene(rec: rr.RecordingStream, mjcf: MjcfTree) -> None:
    """Logs what does not change: the robot model and where the sensors sit."""
    rec.send_blueprint(blueprint(mjcf))

    # The geometry and the frame of every body in the MJCF model.
    mjcf.log_to_recording(rec)

    rec.log(
        "camera",
        rr.Transform3D(
            translation=CAMERA_POSITION,
            mat3x3=CAMERA_ROTATION,
            parent_frame=BASE_BODY,
            child_frame=CAMERA_FRAME,
        ),
        rr.Pinhole(
            focal_length=float(0.5 * CAMERA_WIDTH / np.tan(CAMERA_FOV_X / 2)),
            width=CAMERA_WIDTH,
            height=CAMERA_HEIGHT,
            camera_xyz=rr.ViewCoordinates.RUB,
            image_plane_distance=0.3,
            parent_frame=CAMERA_FRAME,
            child_frame=f"{CAMERA_FRAME}_image",
        ),
        rr.CoordinateFrame(f"{CAMERA_FRAME}_image"),
        static=True,
    )

    # The robot knows where it is, so it places the lidar's points in the world. Points in the
    # lidar's own frame would move with the robot in the Viewer instead of staying where they were seen.
    rec.log("lidar", rr.CoordinateFrame("world"), static=True)

    for path in VELOCITY_PATHS:
        rec.log(f"{path}/commanded", rr.SeriesLines(colors=[120, 120, 120], names="commanded"), static=True)
        rec.log(f"{path}/measured", rr.SeriesLines(names="measured"), static=True)
    rec.log("action", rr.SeriesLines(names=POLICY_JOINTS), static=True)


def newest_datagram(commands: socket.socket) -> bytes | None:
    """The last datagram that arrived since the previous call, if any."""
    newest = None
    while True:
        try:
            newest = commands.recv(64)
        except BlockingIOError:
            return newest


def run(rec: rr.RecordingStream, commands: socket.socket) -> None:
    """
    Walks the robot in real time, following the commands and logging every control step.

    The episode ends when the robot falls over or the program is interrupted.
    """
    model = load_model()
    policy = ort.InferenceSession(POLICY_ONNX, providers=["CPUExecutionProvider"])
    renderer = mujoco.Renderer(model, CAMERA_HEIGHT, CAMERA_WIDTH)

    # Rerun reads the same MJCF file as MuJoCo.
    mjcf = MjcfTree.from_file_path(SCENE_XML)
    log_scene(rec, mjcf)
    hinge_joints = [
        (joint, model.joint(joint.name).qposadr[0]) for joint in mjcf.joints() if joint.joint_type == "hinge"
    ]

    # The Go2 model orders its legs differently from the policy.
    policy_qpos = [model.joint(f"{name}_joint").qposadr[0] for name in POLICY_JOINTS]
    policy_qvel = [model.joint(f"{name}_joint").dofadr[0] for name in POLICY_JOINTS]
    policy_ctrl = [model.actuator(name).id for name in POLICY_JOINTS]

    data = mujoco.MjData(model)
    base = model.body(BASE_BODY).id
    substeps = round(CONTROL_DT / model.opt.timestep)

    mujoco.mj_resetDataKeyframe(model, data, model.keyframe("home").id)
    mujoco.mj_forward(model, data)
    action = np.zeros(len(POLICY_JOINTS), dtype=np.float32)

    stop = np.zeros(3, dtype=np.float32)
    command = stop
    command_heard_at = time.perf_counter()
    next_step_at = time.perf_counter()

    has_fallen = False
    num_steps = 0
    num_images = 0
    num_clouds = 0
    forward_error_sum = 0.0
    try:
        while not has_fallen:
            datagram = newest_datagram(commands)
            if datagram is not None:
                command_heard_at = time.perf_counter()
                valid = len(datagram) == struct.calcsize(COMMAND_FORMAT)
                command = np.array(struct.unpack(COMMAND_FORMAT, datagram), dtype=np.float32) if valid else stop
            elif COMMAND_TIMEOUT_SECS < time.perf_counter() - command_heard_at:
                command = stop

            # The observation layout the policy was trained with in MuJoCo Playground.
            base_from_world = data.xmat[base].reshape(3, 3).T
            observation = np.hstack([
                base_from_world @ data.sensor("global_linvel").data,
                data.sensor("gyro").data,
                base_from_world @ [0.0, 0.0, -1.0],
                data.qpos[policy_qpos] - POLICY_DEFAULT_ANGLES,
                data.qvel[policy_qvel],
                action,
                command,
            ]).astype(np.float32)
            action = policy.run(["continuous_actions"], {"obs": observation[None]})[0][0]
            data.ctrl[policy_ctrl] = POLICY_DEFAULT_ANGLES + ACTION_SCALE * action
            mujoco.mj_step(model, data, substeps)

            sim_secs = num_steps * CONTROL_DT
            rec.set_time("sim_time", duration=sim_secs)

            # The joint transforms take the values of MuJoCo's `qpos` as they are.
            for joint, qpos_address in hinge_joints:
                rec.log("tf", joint.compute_transform(float(data.qpos[qpos_address]), clamp=False))
            # The floating base is a free joint: its `qpos` is a position and a `wxyz` quaternion.
            rec.log(
                "tf",
                rr.Transform3D(
                    translation=data.qpos[0:3],
                    quaternion=rr.Quaternion(xyzw=data.qpos[[4, 5, 6, 3]]),
                    parent_frame="world",
                    child_frame=BASE_BODY,
                ),
            )

            for sensor_id in range(model.nsensor):
                rec.log(f"sensors/{model.sensor(sensor_id).name}", rr.Scalars(data.sensor(sensor_id).data))

            world_from_base = data.xmat[base].reshape(3, 3)
            velocity = world_from_base.T @ data.sensor("global_linvel").data
            measured = [velocity[0], velocity[1], data.sensor("gyro").data[2]]
            for path, commanded_value, measured_value in zip(VELOCITY_PATHS, command, measured):
                rec.log(f"{path}/commanded", rr.Scalars(commanded_value))
                rec.log(f"{path}/measured", rr.Scalars(measured_value))
            up_z = world_from_base[2, 2]
            rec.log("upright", rr.Scalars(up_z))
            rec.log("action", rr.Scalars(action))

            # The camera and the lidar each run at their own rate.
            if num_images / CAMERA_FPS <= sim_secs:
                renderer.update_scene(data, camera=CAMERA_NAME)
                rec.log("camera", rr.Image(renderer.render()))
                num_images += 1
            if num_clouds * LIDAR_SCANS_PER_CLOUD / LIDAR_REFLECTOR_HZ <= sim_secs:
                rec.log("lidar", lidar_cloud(model, data, num_clouds))
                num_clouds += 1

            has_fallen = up_z < FALLEN_UP_Z
            if has_fallen:
                rec.log("events", rr.TextLog("The robot fell over", level=rr.TextLogLevel.ERROR))
                print("The robot fell over. Start it again for a new episode.")

            num_steps += 1
            forward_error_sum += abs(measured[0] - command[0])
            next_step_at += CONTROL_DT
            time.sleep(max(0.0, next_step_at - time.perf_counter()))
    except KeyboardInterrupt:
        pass

    # Properties end up as columns of a dataset's segment table.
    rec.send_property(
        "episode",
        rr.AnyValues(
            success=not has_fallen,
            survived_seconds=num_steps * CONTROL_DT,
            tracking_error=forward_error_sum / max(num_steps, 1),
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--proxy",
        default="rerun+http://127.0.0.1:9876/proxy",
        help="Rerun proxy to log to: a viewer, or `rerun --serve-grpc`.",
    )
    parser.add_argument("--save", type=Path, help="Also write the recording to this `.rrd` file.")
    parser.add_argument(
        "--commands-from-udp",
        default="127.0.0.1:9870",
        help="Where to listen for commands: the address given to `rerun-livekit robot --commands-to-udp`.",
    )
    args = parser.parse_args()

    download_model()
    download(POLICY_URL, POLICY_ONNX)

    host, port = args.commands_from_udp.rsplit(":", 1)
    commands = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    commands.bind((host, int(port)))
    commands.setblocking(False)

    # A recording per episode, in the same application: every `.rrd` becomes a segment of one dataset.
    episode = time.strftime("teleop_%Y%m%d_%H%M%S")
    rec = rr.RecordingStream(
        "rerun_example_livekit_mujoco_robot",
        recording_id=episode,
        # With a file sink the stream would otherwise flush at the file's slower rate.
        batcher_config=rr.ChunkBatcherConfig.LOW_LATENCY(),
    )
    if args.save is None:
        rec.connect_grpc(args.proxy)
    else:
        rec.set_sinks(rr.GrpcSink(args.proxy), rr.FileSink(args.save))
    rec.send_recording_name(episode)

    run(rec, commands)


if __name__ == "__main__":
    main()
