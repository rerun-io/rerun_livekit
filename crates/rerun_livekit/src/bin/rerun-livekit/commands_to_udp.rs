//! `rerun-livekit robot --commands-to-udp`: hands the driver's commands to a robot stack that is
//! not written in Rust.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context as _;
use rerun_livekit::RoomArgs;
use rerun_livekit::control::{DEFAULT_WATCHDOG_TIMEOUT, receive_commands};
use tokio::net::UdpSocket;

/// How often the current command is sent.
const SEND_INTERVAL: Duration = Duration::from_millis(33);

/// Starts sending the command the robot should follow right now to `to`, at a fixed rate, as
/// the bytes the driver sent.
///
/// An empty datagram means stop: nobody is driving, or the driver's commands stopped arriving.
/// The receiver must also stop when datagrams stop, which covers this program dying.
///
/// Call it once. It keeps running across sessions of the bridge.
pub async fn start(room: RoomArgs, to: SocketAddr) -> anyhow::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .context("Failed to open a UDP socket")?;
    let command = receive_commands(room, DEFAULT_WATCHDOG_TIMEOUT);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SEND_INTERVAL);
        loop {
            tick.tick().await;
            let datagram = command.borrow().clone().unwrap_or_default();
            // Nothing listening is not fatal: the robot stack may start after this program.
            if let Err(err) = socket.send_to(&datagram, to).await {
                log::debug!("Failed to send the command to {to}: {err}");
            }
        }
    });
    Ok(())
}
