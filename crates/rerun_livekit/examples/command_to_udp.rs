//! Hands the commands from the LiveKit room to a robot stack that is not written in Rust.
//!
//! Sends the command the robot should follow right now to a UDP address, at a fixed rate, as
//! the bytes the driver sent. An empty datagram means stop: nobody is driving, or the
//! driver's commands stopped arriving. The receiver must also stop when datagrams stop, which
//! covers this program dying.

use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use anyhow::Context as _;
use clap::Parser as _;
use rerun_livekit::RoomArgs;
use rerun_livekit::control::receive_commands;

/// How often the current command is sent on.
const SEND_INTERVAL: Duration = Duration::from_millis(33);

/// The robot stops when commands stop arriving for this long.
const WATCHDOG_TIMEOUT: Duration = Duration::from_millis(300);

#[derive(Debug, clap::Parser)]
struct Args {
    /// Where the robot stack listens for commands.
    #[arg(long, default_value = "127.0.0.1:9870")]
    to: SocketAddr,

    #[command(flatten)]
    room: RoomArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rerun_livekit::init_logging(log::LevelFilter::Info);
    let args = Args::parse();

    let socket = UdpSocket::bind("0.0.0.0:0").context("Failed to open a UDP socket")?;
    let command = receive_commands(args.room, WATCHDOG_TIMEOUT);
    let mut tick = tokio::time::interval(SEND_INTERVAL);

    loop {
        tick.tick().await;
        let datagram = command.borrow().clone().unwrap_or_default();
        // Nothing listening is not fatal: the robot stack may start after this program.
        if let Err(err) = socket.send_to(&datagram, args.to) {
            log::debug!("Failed to send the command to {}: {err}", args.to);
        }
    }
}
