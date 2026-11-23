//! Drives a robot from the keyboard, as an example of sending commands through the room.
//!
//! The robot moves while a key is held and stops when it is let go.
//!
//! The command here is a forward speed and a speed to the left in m/s and a turn rate in
//! rad/s, as three little-endian `f32`, which is what `mujoco_robot.py` reads.
//! [`Driver::send`] takes any bytes: to send something else (a gamepad state, joint targets,
//! a controller pose), encode it and send it the same way.

use std::io::stdout;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use clap::Parser as _;
use crossterm::event::{
    Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, KeyboardEnhancementFlags,
    PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use futures_util::StreamExt as _;
use rerun_livekit::RoomArgs;
use rerun_livekit::control::Driver;

/// How often to send the command. The robot stops if none arrives within its watchdog timeout.
const COMMAND_INTERVAL: Duration = Duration::from_millis(33);

/// How long a key counts as held after it was pressed, when the terminal does not report
/// releases. Longer than the delay before a held key starts repeating: 660 ms by default on
/// X11, less on macOS and Windows.
const FIRST_REPEAT_TIMEOUT: Duration = Duration::from_millis(700);

/// How long a repeating key counts as held after its last repeat, when the terminal does not
/// report releases. Longer than the time between repeats, which is 90 ms by default on macOS.
const REPEAT_TIMEOUT: Duration = Duration::from_millis(200);

const HELP: &str =
    "Hold w/x: forward/back   z/c: sideways left/right   a/d: turn left/right   q: quit";

#[derive(Debug, clap::Parser)]
struct Args {
    /// Speed while a forward, back or sideways key is held, in m/s.
    #[arg(long, default_value_t = 0.4)]
    linear_speed: f32,

    /// Turn rate while a turn key is held, in rad/s.
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
        let terminal = Terminal::enable()?;
        drive(&mut driver, &args, terminal.key_release).await
    };
    driver.release().await;
    result
}

async fn drive(driver: &mut Driver, args: &Args, key_release: KeyRelease) -> anyhow::Result<()> {
    let mut keys = EventStream::new();
    let mut tick = tokio::time::interval(COMMAND_INTERVAL);
    let (mut forward, mut left, mut turn) = (Axis::default(), Axis::default(), Axis::default());

    loop {
        tokio::select! {
            _ = tick.tick() => {
                let now = Instant::now();
                let command = [
                    forward.value(now) * args.linear_speed,
                    left.value(now) * args.linear_speed,
                    turn.value(now) * args.angular_speed,
                ];
                driver.send(command.map(f32::to_le_bytes).concat())?;
            }

            event = keys.next() => {
                let event = event.context("The terminal closed")?.context("Failed to read the keyboard")?;
                let Event::Key(KeyEvent { code, modifiers, kind, .. }) = event else {
                    continue;
                };
                let (axis, direction) = match code {
                    KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => return Ok(()),
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    KeyCode::Char('w') => (&mut forward, Direction::Positive),
                    KeyCode::Char('x') => (&mut forward, Direction::Negative),
                    KeyCode::Char('z') => (&mut left, Direction::Positive),
                    KeyCode::Char('c') => (&mut left, Direction::Negative),
                    KeyCode::Char('a') => (&mut turn, Direction::Positive),
                    KeyCode::Char('d') => (&mut turn, Direction::Negative),
                    _ => continue,
                };
                match kind {
                    KeyEventKind::Press | KeyEventKind::Repeat => axis.press(direction, key_release),
                    KeyEventKind::Release => axis.release(direction),
                }
            }
        }
    }
}

/// How this program learns that a key was let go.
#[derive(Clone, Copy)]
enum KeyRelease {
    /// The terminal reports it.
    Reported,

    /// The terminal only reports presses, and repeats them while a key is held, so a key
    /// was let go when its repeats stop.
    Inferred,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Direction {
    Positive,
    Negative,
}

/// One way the robot can move, driven by a pair of opposite keys.
#[derive(Default)]
struct Axis {
    held: Option<HeldKey>,
}

struct HeldKey {
    direction: Direction,

    /// When to count the key as let go if no repeat arrives. `None` if the terminal reports
    /// the release.
    expires_at: Option<Instant>,
}

impl Axis {
    fn press(&mut self, direction: Direction, key_release: KeyRelease) {
        let expires_at = match key_release {
            KeyRelease::Reported => None,
            KeyRelease::Inferred => {
                let repeating = self
                    .held
                    .as_ref()
                    .is_some_and(|held| held.direction == direction);
                let timeout = if repeating {
                    REPEAT_TIMEOUT
                } else {
                    FIRST_REPEAT_TIMEOUT
                };
                Some(Instant::now() + timeout)
            }
        };
        self.held = Some(HeldKey {
            direction,
            expires_at,
        });
    }

    fn release(&mut self, direction: Direction) {
        if self
            .held
            .as_ref()
            .is_some_and(|held| held.direction == direction)
        {
            self.held = None;
        }
    }

    /// 1 or -1 while a key of this axis is held, else 0.
    fn value(&mut self, now: Instant) -> f32 {
        if self
            .held
            .as_ref()
            .and_then(|held| held.expires_at)
            .is_some_and(|expires_at| expires_at <= now)
        {
            self.held = None;
        }
        match self.held.as_ref().map(|held| held.direction) {
            Some(Direction::Positive) => 1.0,
            Some(Direction::Negative) => -1.0,
            None => 0.0,
        }
    }
}

/// Puts the terminal in raw mode, so keys arrive without Enter, until dropped.
struct Terminal {
    key_release: KeyRelease,
}

impl Terminal {
    fn enable() -> anyhow::Result<Self> {
        crossterm::terminal::enable_raw_mode()
            .context("Failed to read single keys from the terminal")?;
        // From here on, dropping `terminal` restores the terminal.
        let mut terminal = Self {
            key_release: KeyRelease::Inferred,
        };

        // A terminal that does not answer the question does not report releases either.
        if crossterm::terminal::supports_keyboard_enhancement().unwrap_or(false) {
            crossterm::execute!(
                stdout(),
                PushKeyboardEnhancementFlags(
                    KeyboardEnhancementFlags::REPORT_EVENT_TYPES
                        // Without this, keys that type a character have no release event.
                        | KeyboardEnhancementFlags::REPORT_ALL_KEYS_AS_ESCAPE_CODES,
                )
            )
            .context("Failed to ask the terminal for key releases")?;
            terminal.key_release = KeyRelease::Reported;
        }
        Ok(terminal)
    }
}

impl Drop for Terminal {
    fn drop(&mut self) {
        match self.key_release {
            KeyRelease::Reported => {
                crossterm::execute!(stdout(), PopKeyboardEnhancementFlags).ok();
            }
            KeyRelease::Inferred => {}
        }
        crossterm::terminal::disable_raw_mode().ok();
    }
}
