//! A simulated robot to try the bridge with: it logs to Rerun like any robot would, and
//! drives around according to the commands it receives through the LiveKit room.
//!
//! The Rerun side is the stock SDK. It knows nothing about LiveKit.
//!
//! A command is whatever bytes the robot and its driver agree on. This robot takes what
//! `keyboard_teleop` sends: a forward speed in m/s and a turn rate in rad/s, as two
//! little-endian `f32`.

use std::f32::consts::FRAC_PI_2;
use std::ops::Range;
use std::time::Duration;

use clap::Parser as _;
use rerun_livekit::RoomArgs;
use rerun_livekit::control::receive_commands;

/// How often the simulation steps and logs.
const STEP: Duration = Duration::from_millis(33);

/// The robot stops when commands stop arriving for this long.
const WATCHDOG_TIMEOUT: Duration = Duration::from_millis(300);

const CAMERA_WIDTH: u16 = 640;
const CAMERA_HEIGHT: u16 = 360;

/// Horizontal field of view of the camera, in radians.
const CAMERA_FOV: f32 = FRAC_PI_2;

#[derive(Debug, clap::Parser)]
struct Args {
    /// Rerun proxy to log to: a viewer, or `rerun --serve-grpc`.
    #[arg(long, default_value = "rerun+http://127.0.0.1:9876/proxy")]
    proxy: String,

    #[command(flatten)]
    room: RoomArgs,
}

/// A coloured cube standing on the ground.
struct Obstacle {
    position: [f32; 2],
    half_size: f32,
    color: [u8; 3],
}

const OBSTACLES: [Obstacle; 5] = [
    Obstacle {
        position: [4.0, 0.0],
        half_size: 0.5,
        color: [220, 60, 60],
    },
    Obstacle {
        position: [3.0, 3.0],
        half_size: 0.4,
        color: [60, 200, 90],
    },
    Obstacle {
        position: [-2.0, 4.0],
        half_size: 0.6,
        color: [70, 110, 230],
    },
    Obstacle {
        position: [-4.0, -2.0],
        half_size: 0.5,
        color: [230, 200, 60],
    },
    Obstacle {
        position: [2.0, -4.0],
        half_size: 0.3,
        color: [200, 80, 220],
    },
];

#[derive(Default)]
struct Pose {
    position: [f32; 2],

    /// Heading in radians, counter-clockwise from +X.
    yaw: f32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    let rec = rerun::RecordingStreamBuilder::new("rerun_example_livekit_sim_robot")
        .connect_grpc_opts(args.proxy)?;
    let command = receive_commands(args.room, WATCHDOG_TIMEOUT);

    rec.log_static(
        "world/obstacles",
        &rerun::Boxes3D::from_centers_and_half_sizes(
            OBSTACLES
                .iter()
                .map(|o| [o.position[0], o.position[1], o.half_size]),
            OBSTACLES.iter().map(|o| [o.half_size; 3]),
        )
        .with_colors(
            OBSTACLES
                .iter()
                .map(|o| rerun::Color::from_rgb(o.color[0], o.color[1], o.color[2])),
        )
        .with_fill_mode(rerun::FillMode::Solid),
    )?;
    rec.log_static(
        "world/robot/body",
        &rerun::Boxes3D::from_centers_and_half_sizes([[0.0, 0.0, 0.15]], [[0.3, 0.2, 0.15]]),
    )?;

    // The camera looks along the robot's X axis, from above its body.
    let focal_px = focal_length_px();
    rec.log_static(
        "world/robot/camera",
        &rerun::Transform3D::from_translation([0.3, 0.0, 0.5]),
    )?;
    rec.log_static(
        "world/robot/camera",
        &rerun::Pinhole::from_focal_length_and_resolution(
            [focal_px, focal_px],
            [f32::from(CAMERA_WIDTH), f32::from(CAMERA_HEIGHT)],
        )
        .with_camera_xyz(rerun::components::ViewCoordinates::FLU)
        .with_image_plane_distance(0.5),
    )?;

    let mut pose = Pose::default();
    let mut tick = tokio::time::interval(STEP);
    for step in 0_u32.. {
        tick.tick().await;
        // No command, or one this robot does not understand, means stop.
        let (forward, turn) = command
            .borrow()
            .as_deref()
            .and_then(decode_command)
            .unwrap_or((0.0, 0.0));

        let dt = STEP.as_secs_f32();
        pose.yaw += turn * dt;
        pose.position[0] += forward * pose.yaw.cos() * dt;
        pose.position[1] += forward * pose.yaw.sin() * dt;

        rec.set_duration_secs("sim_time", f64::from(step) * STEP.as_secs_f64());
        rec.log(
            "world/robot",
            &rerun::Transform3D::from_translation_rotation(
                [pose.position[0], pose.position[1], 0.0],
                rerun::RotationAxisAngle::new(
                    [0.0, 0.0, 1.0],
                    rerun::Angle::from_radians(pose.yaw),
                ),
            ),
        )?;
        rec.log(
            "command/forward",
            &rerun::Scalars::single(f64::from(forward)),
        )?;
        rec.log("command/turn", &rerun::Scalars::single(f64::from(turn)))?;
        rec.log(
            "world/robot/camera",
            &rerun::Image::from_rgb24(
                render_camera(&pose),
                [u32::from(CAMERA_WIDTH), u32::from(CAMERA_HEIGHT)],
            ),
        )?;
    }
    Ok(())
}

/// Reads the forward speed and turn rate that `keyboard_teleop` sends.
fn decode_command(bytes: &[u8]) -> Option<(f32, f32)> {
    let ([forward, turn], []) = bytes.as_chunks::<4>() else {
        return None;
    };
    Some((f32::from_le_bytes(*forward), f32::from_le_bytes(*turn)))
}

fn focal_length_px() -> f32 {
    f32::from(CAMERA_WIDTH) / 2.0 / (CAMERA_FOV / 2.0).tan()
}

/// Draws what the robot sees: sky, ground, and each obstacle as a flat rectangle.
fn render_camera(pose: &Pose) -> Vec<u8> {
    let (width, height) = (usize::from(CAMERA_WIDTH), usize::from(CAMERA_HEIGHT));
    let horizon = height / 2;
    let mut pixels: Vec<[u8; 3]> = (0..height)
        .flat_map(|y| {
            let color = if y < horizon {
                [150, 190, 230]
            } else {
                [90, 90, 90]
            };
            std::iter::repeat_n(color, width)
        })
        .collect();

    let focal_px = focal_length_px();
    let (sin, cos) = pose.yaw.sin_cos();
    let mut visible: Vec<_> = OBSTACLES
        .iter()
        .filter_map(|obstacle| {
            let dx = obstacle.position[0] - pose.position[0];
            let dy = obstacle.position[1] - pose.position[1];
            // In the robot's frame: `ahead` along its heading, `left` to its left.
            let (ahead, left) = (dx * cos + dy * sin, -dx * sin + dy * cos);
            (0.1 < ahead).then_some((ahead, left, obstacle))
        })
        .collect();
    // Far to near, so that near obstacles cover far ones.
    visible.sort_by(|a, b| b.0.total_cmp(&a.0));

    for (ahead, left, obstacle) in visible {
        let center_x = f32::from(CAMERA_WIDTH) / 2.0 - left / ahead * focal_px;
        // The camera sits at the height of the obstacles' centers.
        let center_y = f32::from(CAMERA_HEIGHT) / 2.0;
        let half_size_px = obstacle.half_size / ahead * focal_px;
        for y in pixel_range(center_y, half_size_px, height) {
            for x in pixel_range(center_x, half_size_px, width) {
                pixels[y * width + x] = obstacle.color;
            }
        }
    }
    pixels.into_flattened()
}

/// The pixels within `half_size` of `center`, cut off at the image's edges.
fn pixel_range(center: f32, half_size: f32, num_pixels: usize) -> Range<usize> {
    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // clamped to the image
    let pixel = |position: f32| (position.max(0.0) as usize).min(num_pixels);
    pixel(center - half_size)..pixel(center + half_size)
}
