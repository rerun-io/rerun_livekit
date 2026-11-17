//! Drives a robot from the keyboard, as an example of sending commands through the room.
//!
//! The command here is a forward speed in m/s and a turn rate in rad/s, as two little-endian
//! `f32`, which is what `sim_robot` reads. [`Driver::send`] takes any bytes: to send something
//! else (a gamepad state, joint targets, a controller pose), encode it and send it the same way.

use std::time::Duration;

use anyhow::Context as _;
use clap::Parser as _;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures_util::StreamExt as _;
use rerun_livekit::RoomArgs;
use rerun_livekit::control::Driver;

/// How often to send the command. The robot stops if none arrives within its watchdog timeout.
const COMMAND_INTERVAL: Duration = Duration::from_millis(33);

const HELP: &str = "w/x: faster/slower   a/d: turn left/right   s or space: stop   q: quit";

/// How much one key press changes the forward speed, in m/s.
const LINEAR_STEP: f32 = 0.1;

/// How much one key press changes the turn rate, in rad/s.
const ANGULAR_STEP: f32 = 0.2;

#[derive(Debug, clap::Parser)]
struct Args {
    #[command(flatten)]
    room: RoomArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    let args = Args::parse();

    let mut driver = Driver::acquire(&args.room).await?;
    #[expect(clippy::print_stdout)] // this is a terminal program
    {
        println!("You are driving. {HELP}");
    }

    let result = {
        let _raw_mode = RawMode::enable()?;
        drive(&mut driver).await
    };
    driver.release().await;
    result
}

async fn drive(driver: &mut Driver) -> anyhow::Result<()> {
    let mut keys = EventStream::new();
    let mut tick = tokio::time::interval(COMMAND_INTERVAL);
    let (mut forward, mut turn) = (0.0_f32, 0.0_f32);

    loop {
        tokio::select! {
            _ = tick.tick() => driver.send([forward.to_le_bytes(), turn.to_le_bytes()].concat())?,

            event = keys.next() => {
                let event = event.context("The terminal closed")?.context("Failed to read the keyboard")?;
                let Event::Key(KeyEvent { code, modifiers, kind: KeyEventKind::Press, .. }) = event else {
                    continue;
                };
                match code {
                    KeyCode::Char('w') => forward += LINEAR_STEP,
                    KeyCode::Char('x') => forward -= LINEAR_STEP,
                    KeyCode::Char('a') => turn += ANGULAR_STEP,
                    KeyCode::Char('d') => turn -= ANGULAR_STEP,
                    KeyCode::Char('s' | ' ') => (forward, turn) = (0.0, 0.0),
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

/// Puts the terminal in raw mode, so keys arrive without Enter, until dropped.
struct RawMode;

impl RawMode {
    fn enable() -> anyhow::Result<Self> {
        crossterm::terminal::enable_raw_mode()
            .context("Failed to read single keys from the terminal")?;
        Ok(Self)
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        crossterm::terminal::disable_raw_mode().ok();
    }
}
