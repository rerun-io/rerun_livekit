//! Command line for the Rerun and LiveKit bridge. See the crate docs of `rerun_livekit`.

use std::convert::Infallible;
use std::time::Duration;

use clap::Parser as _;
use rerun_livekit::{operator, robot};

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
    rerun_livekit::init_logging(log::LevelFilter::Info);

    let command = Command::parse();
    if matches!(command, Command::Operator(_)) {
        // Show each video frame as soon as it is decoded instead of smoothing playback.
        // This is process-wide and only possible before the first connection.
        livekit::webrtc::enable_zero_playout_delay()
            .map_err(|err| anyhow::anyhow!("Failed to turn off the playout delay: {err}"))?;
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

/// Runs sessions back to back, forever.
///
/// Starting over is the only recovery mechanism: a new session reconnects both ends, and
/// joining the room again makes the robot replay what a newcomer needs.
async fn retry<F>(mut run_session: impl FnMut() -> F) -> !
where
    F: Future<Output = anyhow::Result<Infallible>>,
{
    loop {
        let Err(err) = run_session().await;
        log::warn!("Session ended, starting a new one in {SESSION_RETRY_DELAY:?}: {err:#}");
        tokio::time::sleep(SESSION_RETRY_DELAY).await;
    }
}
