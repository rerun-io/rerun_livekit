//! The bridge: carries a Rerun stream from a robot through a LiveKit room to operators.
//!
//! [`robot`] reads the stream from a Rerun proxy next to the robot and publishes it to a room.
//! [`operator`] receives it from the room and writes it to a Viewer next to the operator.

mod classify;
mod commands_to_udp;
mod latency;
mod logging;
mod operator;
mod proxy;
mod robot;
mod rules;
mod video;
mod wire;

use std::convert::Infallible;
use std::time::Duration;

use clap::Parser as _;
use livekit::prelude::RoomEvent;
use tokio::sync::mpsc::UnboundedReceiver;

/// How long to wait before starting a new session after one ended.
const SESSION_RETRY_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug, clap::Parser)]
#[command(version, about)]
enum Command {
    /// Publish the Rerun stream of a robot to the room.
    Robot(robot::RobotArgs),

    /// Show the Rerun stream from the room in a viewer.
    Operator(operator::OperatorArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init_logging(log::LevelFilter::Info);

    let command = Command::parse();
    if matches!(command, Command::Operator(_)) {
        // Show each video frame as soon as it is decoded instead of smoothing playback.
        // This is process-wide and only possible before the first connection.
        livekit::webrtc::enable_zero_playout_delay()
            .map_err(|err| anyhow::anyhow!("Failed to turn off the playout delay: {err}"))?;
    }

    if let Command::Robot(args) = &command
        && let Some(to) = args.commands_to_udp
    {
        commands_to_udp::start(args.room.clone(), to).await?;
    }

    let run = async {
        match command {
            Command::Robot(args) => retry(|| robot::run_session(&args)).await,
            Command::Operator(args) => retry(|| operator::run_session(&args)).await,
        }
    };
    tokio::select! {
        result = run => result,
        _ = tokio::signal::ctrl_c() => Ok(()),
    }
}

/// The error that ended a session, or the room's own disconnect if that is what ended it.
///
/// LiveKit queues its `Disconnected` event before it closes the connection, so when being
/// disconnected made the session fail, for example by failing a send, the event is already
/// in `events`, whichever branch of the session saw the failure first. Call it before closing
/// the room, which queues a `Disconnected` of its own.
fn session_error(err: anyhow::Error, events: &mut UnboundedReceiver<RoomEvent>) -> anyhow::Error {
    while let Ok(event) = events.try_recv() {
        if let RoomEvent::Disconnected { reason } = event {
            return rerun_livekit::Error::Disconnected { reason }.into();
        }
    }
    err
}

/// Runs sessions back to back, until another participant takes this one's place in the room.
///
/// Starting over is the only recovery mechanism: a new session reconnects both ends, and
/// joining the room again makes the robot replay what a newcomer needs.
async fn retry<F>(mut run_session: impl FnMut() -> F) -> anyhow::Result<()>
where
    F: Future<Output = anyhow::Result<Infallible>>,
{
    loop {
        let Err(err) = run_session().await;
        if err
            .downcast_ref::<rerun_livekit::Error>()
            .is_some_and(rerun_livekit::Error::is_replaced)
        {
            return Err(err.context("Another participant joined the room with this identity"));
        }
        log::warn!("Session ended, starting a new one in {SESSION_RETRY_DELAY:?}: {err:#}");
        tokio::time::sleep(SESSION_RETRY_DELAY).await;
    }
}

#[cfg(test)]
mod tests {
    use livekit::DisconnectReason;
    use tokio::sync::mpsc;

    use super::*;

    #[test]
    fn a_queued_disconnect_is_what_ended_the_session() {
        let (events_tx, mut events) = mpsc::unbounded_channel();
        events_tx.send(RoomEvent::Reconnecting).unwrap();
        events_tx
            .send(RoomEvent::Disconnected {
                reason: DisconnectReason::DuplicateIdentity,
            })
            .unwrap();

        let err = session_error(anyhow::anyhow!("Failed to send"), &mut events);
        assert!(
            err.downcast_ref::<rerun_livekit::Error>()
                .is_some_and(rerun_livekit::Error::is_replaced)
        );
    }

    #[test]
    fn without_a_disconnect_the_error_stays() {
        let (events_tx, mut events) = mpsc::unbounded_channel();
        events_tx.send(RoomEvent::Reconnecting).unwrap();

        let err = session_error(anyhow::anyhow!("Lost the Rerun proxy"), &mut events);
        assert_eq!(err.to_string(), "Lost the Rerun proxy");
    }
}
