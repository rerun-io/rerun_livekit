//! Drives a robot from the keyboard, as an example of sending commands through the room.
//!
//! A key sets how the robot moves, and it keeps moving that way until another key changes
//! it. A terminal does not reliably say when a key is let go, so holding a key is not used.
//!
//! The command here is a forward speed and a speed to the left in m/s and a turn rate in
//! rad/s, as three little-endian `f32`, which is what `mujoco_robot.py` reads.
//! [`Driver::send`] takes any bytes: to send something else (a gamepad state, joint targets,
//! a controller pose), encode it and send it the same way.

use std::time::Duration;

use anyhow::Context as _;
use clap::Parser as _;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures_util::StreamExt as _;
use rerun_livekit::RoomArgs;
use rerun_livekit::control::Driver;

/// How often to send the command. The robot stops if none arrives within its watchdog timeout.
const COMMAND_INTERVAL: Duration = Duration::from_millis(33);

const HELP: &str =
    "w/x: forward/back   z/c: sideways left/right   a/d: turn left/right   space: stop   q: quit";

#[derive(Debug, clap::Parser)]
struct Args {
    /// Speed after a forward, back or sideways key, in m/s.
    #[arg(long, default_value_t = 0.4)]
    linear_speed: f32,

    /// Turn rate after a turn key, in rad/s.
    #[arg(long, default_value_t = 0.8)]
    angular_speed: f32,

    #[command(flatten)]
    room: RoomArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rerun_livekit::init_logging(log::LevelFilter::Warn);
    let args = Args::parse();

    let mut driver = Driver::acquire(&args.room).await?;
    #[expect(clippy::print_stdout)] // this is a terminal program
    {
        println!("You are driving. {HELP}");
    }

    let result = {
        let _terminal = RawTerminal::enable()?;
        drive(&mut driver, &args).await
    };
    driver.release().await;
    result
}

async fn drive(driver: &mut Driver, args: &Args) -> anyhow::Result<()> {
    let mut keys = EventStream::new();
    let mut tick = tokio::time::interval(COMMAND_INTERVAL);

    let mut motion = Motion::default();

    loop {
        tokio::select! {
            _ = tick.tick() => {
                let Motion { forward, left, turn } = motion;
                let command = [
                    forward * args.linear_speed,
                    left * args.linear_speed,
                    turn * args.angular_speed,
                ];
                driver.send(command.map(f32::to_le_bytes).concat())?;
            }

            event = keys.next() => {
                let event = event.context("The terminal closed")?.context("Failed to read the keyboard")?;
                let Event::Key(KeyEvent { code, modifiers, kind, .. }) = event else {
                    continue;
                };
                if kind == KeyEventKind::Release {
                    continue;
                }
                // A key moves its axis one step: `w` then `x` stops going forward.
                let (axis, step) = match code {
                    KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => return Ok(()),
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    KeyCode::Char(' ') => {
                        motion = Motion::default();
                        continue;
                    }
                    KeyCode::Char('w') => (&mut motion.forward, 1.0),
                    KeyCode::Char('x') => (&mut motion.forward, -1.0),
                    KeyCode::Char('z') => (&mut motion.left, 1.0),
                    KeyCode::Char('c') => (&mut motion.left, -1.0),
                    KeyCode::Char('a') => (&mut motion.turn, 1.0),
                    KeyCode::Char('d') => (&mut motion.turn, -1.0),
                    _ => continue,
                };
                *axis = (*axis + step).clamp(-1.0, 1.0);
            }
        }
    }
}

/// How the keys say the robot should move. Each axis is -1, 0 or 1.
#[derive(Clone, Copy, Default)]
struct Motion {
    forward: f32,
    left: f32,

    /// Counter-clockwise.
    turn: f32,
}

/// Puts the terminal in raw mode, so keys arrive without Enter, until dropped.
struct RawTerminal;

impl RawTerminal {
    fn enable() -> anyhow::Result<Self> {
        crossterm::terminal::enable_raw_mode()
            .context("Failed to read single keys from the terminal")?;
        Ok(Self)
    }
}

impl Drop for RawTerminal {
    fn drop(&mut self) {
        crossterm::terminal::disable_raw_mode().ok();
    }
}
