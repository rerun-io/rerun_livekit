#!/usr/bin/env python3
"""
A simulated humanoid to teleoperate through the bridge.

A Unitree G1 walks in MuJoCo, driven by a pretrained policy from MuJoCo Playground. It logs to
Rerun like any robot would, with the stock SDK, and knows nothing about LiveKit: it follows the
commands that `rerun-livekit robot --commands-to-udp` hands it.

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

G1_DIR = Path(__file__).parent / "g1"

PLAYGROUND_URL = "https://raw.githubusercontent.com/google-deepmind/mujoco_playground/e0842145be50291c4c47c7ffc0424e32d5fdd6eb/rss2025/mujoco_playground"
MENAGERIE_URL = (
    "https://raw.githubusercontent.com/google-deepmind/mujoco_menagerie/370c43965323c0944b433b202aac5058b5f907a9"
)
G1_XML_URL = f"{PLAYGROUND_URL}/_src/locomotion/g1/xmls"
MESH_DIR = "../../../../../mujoco_menagerie/unitree_g1/assets"
G1_FILES = {
    "scene_mjx_feetonly_flat_terrain.xml": G1_XML_URL,
    "g1_mjx_feetonly.xml": G1_XML_URL,
    "sensor.xml": G1_XML_URL,
    "g1_policy.onnx": f"{PLAYGROUND_URL}/experimental/sim2sim/onnx",
}
SCENE_XML = G1_DIR / "scene_mjx_feetonly_flat_terrain.xml"
ROBOT_XML = G1_DIR / "g1_mjx_feetonly.xml"
POLICY_ONNX = G1_DIR / "g1_policy.onnx"

CONTROL_DT = 0.02
ACTION_SCALE = 0.5
GAIT_FREQUENCY = 1.5  # Steps per second of each foot, which the policy takes as a phase input.
FALLEN_UP_Z = 0.3  # The robot has fallen once its up axis tilts more than ~70° away from vertical.

# The command goes back to zero when datagrams stop arriving for this long.
COMMAND_TIMEOUT_SECS = 0.3
COMMAND_FORMAT = "<3f"

# Where each part of a command is logged next to what the robot measured, in the order of a
# command: the speed ahead and to the left in m/s, and the turn rate in rad/s.
VELOCITY_PATHS = ("velocity", "lateral_velocity", "turn_rate")

# The head camera, which the G1 model does not have.
CAMERA_NAME = "head"
CAMERA_BODY = "torso_link"
CAMERA_POSITION = (0.1, 0.0, 0.45)  # In the frame of `CAMERA_BODY`: X ahead, Y left, Z up.
CAMERA_TILT_DOWN = np.radians(20.0)
CAMERA_FOV_Y = np.radians(75.0)
# 640 is the widest image MuJoCo renders offscreen unless the model asks for more.
CAMERA_WIDTH = 640
CAMERA_HEIGHT = 360
# LiveKit sends images this wide at up to 25 frames per second.
CONTROL_STEPS_PER_IMAGE = 2

# Coloured boxes to steer towards, as (x, y) and RGB. Nothing collides with them.
LANDMARK_HALF_SIZE = 0.3
LANDMARKS = [
    ((4.0, 0.0), (220, 60, 60)),
    ((3.0, 3.0), (60, 200, 90)),
    ((-2.0, 4.0), (70, 110, 230)),
    ((-4.0, -2.0), (230, 200, 60)),
    ((2.0, -4.0), (200, 80, 220)),
]


def download(url: str, path: Path) -> None:
    if path.exists():
        return
    print(f"Downloading {path.name}…")
    path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=60) as response:
        path.write_bytes(response.read())


def download_g1() -> None:
    """Downloads the G1 model and its policy from MuJoCo Playground and MuJoCo Menagerie."""
    for file, base_url in G1_FILES.items():
        download(f"{base_url}/{file}", G1_DIR / file)

    # MuJoCo Playground loads the meshes from its own cache, we keep them next to the model.
    robot_xml = ROBOT_XML.read_text(encoding="utf-8")
    for mesh in sorted(set(re.findall(rf"{MESH_DIR}/(\w+\.STL)", robot_xml))):
        download(f"{MENAGERIE_URL}/unitree_g1/assets/{mesh}", G1_DIR / "assets" / mesh)
    ROBOT_XML.write_text(robot_xml.replace(MESH_DIR, "assets"), encoding="utf-8")


def camera_rotation() -> np.ndarray:
    """The axes of the head camera as the columns of a matrix, in the frame of `CAMERA_BODY`."""
    # A MuJoCo camera looks along its -Z axis, with X to the right and Y up.
    right = [0.0, -1.0, 0.0]
    up = [np.sin(CAMERA_TILT_DOWN), 0.0, np.cos(CAMERA_TILT_DOWN)]
    return np.column_stack([right, up, np.cross(right, up)])


def load_model() -> mujoco.MjModel:
    """The G1 scene, with the head camera and the landmarks added."""
    spec = mujoco.MjSpec.from_file(str(SCENE_XML))

    quat_wxyz = np.zeros(4)
    mujoco.mju_mat2Quat(quat_wxyz, camera_rotation().flatten())
    spec.body(CAMERA_BODY).add_camera(
        name=CAMERA_NAME, pos=CAMERA_POSITION, quat=quat_wxyz, fovy=np.degrees(CAMERA_FOV_Y)
    )

    for (x, y), color in LANDMARKS:
        spec.worldbody.add_geom(
            type=mujoco.mjtGeom.mjGEOM_BOX,
            size=[LANDMARK_HALF_SIZE] * 3,
            pos=[x, y, LANDMARK_HALF_SIZE],
            rgba=[*(channel / 255 for channel in color), 1.0],
            contype=0,
            conaffinity=0,
        )

    return spec.compile()


def blueprint(mjcf: MjcfTree) -> rrb.Blueprint:
    """A 3D view that follows the robot, next to what it sees and how well it follows the commands."""
    return rrb.Blueprint(
        rrb.Horizontal(
            rrb.Spatial3DView(
                name="G1",
                # MJCF does not mark geoms as collision geometry, so the importer goes by their group.
                contents=["+ /**", f"- /{rr.escape_entity_path_part(mjcf.name)}/collision_geometries/**"],
                eye_controls=rrb.EyeControls3D(
                    position=[-2.5, -2.0, 1.6],
                    look_target=[0.0, 0.0, 0.7],
                    tracking_entity=mjcf.get_visual_geometry_paths(CAMERA_BODY)[0],
                ),
            ),
            rrb.Vertical(
                rrb.Spatial2DView(name="Head camera", origin="/camera"),
                rrb.TimeSeriesView(name="Forward velocity", origin="/velocity"),
                rrb.TimeSeriesView(name="Turn rate", origin="/turn_rate"),
            ),
            column_shares=[3, 2],
        ),
        collapse_panels=True,
    )


def log_scene(rec: rr.RecordingStream, model: mujoco.MjModel, mjcf: MjcfTree) -> None:
    """Logs what does not change: the robot model, the landmarks, and where the camera sits."""
    rec.send_blueprint(blueprint(mjcf))

    # The geometry and the frame of every body in the MJCF model.
    mjcf.log_to_recording(rec)

    rec.log(
        "landmarks",
        rr.Boxes3D(
            centers=[[x, y, LANDMARK_HALF_SIZE] for (x, y), _color in LANDMARKS],
            half_sizes=[[LANDMARK_HALF_SIZE] * 3] * len(LANDMARKS),
            colors=[color for _position, color in LANDMARKS],
            fill_mode="solid",
        ),
        rr.CoordinateFrame("world"),
        static=True,
    )

    rec.log(
        "camera",
        rr.Transform3D(
            translation=CAMERA_POSITION,
            mat3x3=camera_rotation(),
            parent_frame=CAMERA_BODY,
            child_frame="head_camera",
        ),
        rr.Pinhole(
            focal_length=float(0.5 * CAMERA_HEIGHT / np.tan(CAMERA_FOV_Y / 2)),
            width=CAMERA_WIDTH,
            height=CAMERA_HEIGHT,
            camera_xyz=rr.ViewCoordinates.RUB,
            image_plane_distance=0.5,
            parent_frame="head_camera",
            child_frame="head_camera_image",
        ),
        rr.CoordinateFrame("head_camera_image"),
        static=True,
    )

    for path in VELOCITY_PATHS:
        rec.log(f"{path}/commanded", rr.SeriesLines(colors=[120, 120, 120], names="commanded"), static=True)
        rec.log(f"{path}/measured", rr.SeriesLines(names="measured"), static=True)
    rec.log("action", rr.SeriesLines(names=[model.actuator(i).name for i in range(model.nu)]), static=True)


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
    log_scene(rec, model, mjcf)
    hinge_joints = [
        (joint, model.joint(joint.name).qposadr[0]) for joint in mjcf.joints() if joint.joint_type == "hinge"
    ]

    data = mujoco.MjData(model)
    standing = model.keyframe("knees_bent")
    default_angles = standing.qpos[7:]
    imu = model.site("imu_in_pelvis").id
    substeps = round(CONTROL_DT / model.opt.timestep)

    mujoco.mj_resetDataKeyframe(model, data, standing.id)
    action = np.zeros(model.nu, dtype=np.float32)
    gait_phase = np.array([0.0, np.pi])  # Of the left and the right foot.

    stop = np.zeros(3, dtype=np.float32)
    command = stop
    command_heard_at = time.perf_counter()
    next_step_at = time.perf_counter()

    has_fallen = False
    num_steps = 0
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
            # It was not trained to stand still: on a zero command it steps in place and drifts.
            gravity = data.site_xmat[imu].reshape(3, 3).T @ np.array([0.0, 0.0, -1.0])
            observation = np.hstack([
                data.sensor("local_linvel_pelvis").data,
                data.sensor("gyro_pelvis").data,
                gravity,
                command,
                data.qpos[7:] - default_angles,
                data.qvel[6:],
                action,
                np.cos(gait_phase),
                np.sin(gait_phase),
            ]).astype(np.float32)
            action = policy.run(["continuous_actions"], {"obs": observation[None]})[0][0]
            data.ctrl[:] = default_angles + ACTION_SCALE * action
            gait_phase += 2.0 * np.pi * GAIT_FREQUENCY * CONTROL_DT
            mujoco.mj_step(model, data, substeps)

            rec.set_time("sim_time", duration=num_steps * CONTROL_DT)

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
                    child_frame="pelvis",
                ),
            )

            measured = [*data.sensor("local_linvel_pelvis").data[:2], data.sensor("gyro_pelvis").data[2]]
            for path, commanded_value, measured_value in zip(VELOCITY_PATHS, command, measured):
                rec.log(f"{path}/commanded", rr.Scalars(commanded_value))
                rec.log(f"{path}/measured", rr.Scalars(measured_value))
            up_z = data.sensor("upvector_torso").data[2]
            rec.log("upright", rr.Scalars(up_z))
            rec.log("action", rr.Scalars(action))

            if num_steps % CONTROL_STEPS_PER_IMAGE == 0:
                renderer.update_scene(data, camera=CAMERA_NAME)
                rec.log("camera", rr.Image(renderer.render()))

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

    download_g1()

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
