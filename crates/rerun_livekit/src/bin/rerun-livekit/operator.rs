//! The operator side: receives a Rerun stream from the room and writes it to a viewer.

use std::convert::Infallible;

use anyhow::Context as _;
use futures_util::StreamExt as _;
use livekit::StreamReader as _;
use livekit::prelude::*;
use prost::Message as _;
use re_protos::log_msg::v1alpha1::log_msg::Msg;
use re_protos::log_msg::v1alpha1::{ArrowMsg, LogMsg};
use re_protos::sdk_comms::v1alpha1::WriteMessagesRequest;
use rerun::TimePoint;
use rerun::external::re_log_encoding::ToApplication as _;
use rerun::external::re_uri::ProxyUri;
use rerun::log::Chunk;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::latency::{LatencyPlots, LatencyReport};
use rerun_livekit::{Role, RoomArgs};

use crate::video::{TRACK_INFO_TOPIC, TrackInfos};
use crate::wire::{LATEST_TRACK, LOG_TOPIC, LatestFrame};

/// How many messages may wait for the viewer before the bridge stops reading from the room.
const VIEWER_QUEUE_CAPACITY: usize = 1024;

/// How many latest-wins frames LiveKit may hold for us before it drops the oldest.
///
/// LiveKit's default of 16 is a fraction of a second of logging, which a short stall of the
/// viewer would overflow.
const LATEST_BUFFER_FRAMES: usize = 1024;

/// How many fragmented latest-wins frames LiveKit reassembles at once.
const LATEST_PARTIAL_FRAMES: usize = 16;

#[derive(Clone, Debug, clap::Args)]
pub struct OperatorArgs {
    /// Rerun proxy to write the stream to: a viewer, or `rerun --serve-grpc` for a web viewer.
    #[arg(long, default_value = "rerun+http://127.0.0.1:9876/proxy")]
    viewer: ProxyUri,

    /// Plot the latency of each stream in the viewer, under `/livekit/latency_ms`.
    ///
    /// This adds entities to the recording that the robot did not log.
    #[arg(long)]
    plot_latency: bool,

    #[command(flatten)]
    room: RoomArgs,
}

/// Runs one session, which lasts until the viewer or the room goes away.
///
/// # Errors
/// Always, with what ended the session.
pub async fn run_session(args: &OperatorArgs) -> anyhow::Result<Infallible> {
    let mut viewer = crate::proxy::connect(&args.viewer).await?;
    let (room, events) = args.room.connect(Role::Operator).await?;
    log::info!("Forwarding the room to {}", args.viewer);

    let latency_plots = if args.plot_latency {
        LatencyPlots::On
    } else {
        LatencyPlots::Off
    };
    let (viewer_tx, viewer_rx) = mpsc::channel(VIEWER_QUEUE_CAPACITY);
    let result = tokio::select! {
        result = handle_room_events(events, viewer_tx, latency_plots) => result,
        result = viewer.write_messages(ReceiverStream::new(viewer_rx)) => match result {
            Ok(_) => Err(anyhow::anyhow!("The viewer closed the stream")),
            Err(status) => Err(status).context("Lost the viewer"),
        },
    };
    room.close().await.ok();
    result
}

async fn handle_room_events(
    mut events: mpsc::UnboundedReceiver<RoomEvent>,
    viewer_tx: mpsc::Sender<WriteMessagesRequest>,
    latency_plots: LatencyPlots,
) -> anyhow::Result<Infallible> {
    let track_infos = TrackInfos::default();
    loop {
        match events.recv().await.context("The room closed")? {
            RoomEvent::ByteStreamOpened { reader, topic, .. }
                if topic == LOG_TOPIC || topic == TRACK_INFO_TOPIC =>
            {
                let Some(reader) = reader.take() else {
                    continue;
                };
                // Read to the end before taking the next event, so messages stay in order.
                let bytes = reader
                    .read_all()
                    .await
                    .context("Failed to receive a message from the room")?;
                if topic == TRACK_INFO_TOPIC {
                    track_infos.insert(bytes)?;
                    continue;
                }
                let log_msg = LogMsg::decode(bytes).context("Received a malformed log message")?;
                viewer_tx
                    .send(WriteMessagesRequest {
                        log_msg: Some(log_msg),
                    })
                    .await
                    .context("The viewer stream closed")?;
            }
            RoomEvent::TrackSubscribed {
                track: RemoteTrack::Video(track),
                publication,
                ..
            } if crate::video::is_rerun_track(&publication.name()) => {
                tokio::spawn(crate::video::receive_frames(
                    track,
                    publication.name(),
                    track_infos.clone(),
                    viewer_tx.clone(),
                    latency_plots,
                ));
            }
            RoomEvent::DataTrackPublished(track) if track.info().name() == LATEST_TRACK => {
                track.set_pipeline_options(
                    RemoteDataTrackPipelineOptions::new()
                        .with_max_partial_frames(LATEST_PARTIAL_FRAMES),
                );
                let options =
                    DataTrackSubscribeOptions::new().with_buffer_size(LATEST_BUFFER_FRAMES);
                let frames = track
                    .subscribe_with_options(options)
                    .await
                    .context("Failed to subscribe to the data track")?;
                let latency = LatencyReport::new("Latest-wins data").with_plots(
                    latency_plots,
                    &"data".into(),
                    &viewer_tx,
                );
                tokio::spawn(receive_latest(frames, viewer_tx.clone(), latency));
            }
            RoomEvent::Disconnected { reason } => {
                anyhow::bail!("Disconnected from the room: {reason:?}");
            }
            _ => {}
        }
    }
}

/// Forwards latest-wins messages until the track or the viewer goes away.
async fn receive_latest(
    mut frames: DataTrackStream,
    viewer_tx: mpsc::Sender<WriteMessagesRequest>,
    mut latency: LatencyReport,
) {
    while let Some(frame) = frames.next().await {
        let frame = match LatestFrame::decode(frame.payload()) {
            Ok(frame) => frame,
            Err(err) => {
                log::warn!("Received a malformed frame of log messages: {err}");
                continue;
            }
        };
        for bytes in frame.log_msgs {
            let log_msg = match LogMsg::decode(bytes) {
                Ok(log_msg) => log_msg,
                Err(err) => {
                    log::warn!("Received a malformed log message: {err}");
                    continue;
                }
            };
            if let Some(Msg::ArrowMsg(arrow_msg)) = &log_msg.msg
                && let Some(created_nanos) = arrow_msg.chunk_id.as_ref().and_then(|id| id.time_ns)
                && let Some(store_id) = &arrow_msg.store_id
            {
                latency.record(created_nanos, store_id, || newest_timepoint(arrow_msg));
            }
            let request = WriteMessagesRequest {
                log_msg: Some(log_msg),
            };
            if viewer_tx.send(request).await.is_err() {
                return;
            }
        }
    }
}

/// The times of the last row of the chunk in `arrow_msg`.
fn newest_timepoint(arrow_msg: &ArrowMsg) -> Option<TimePoint> {
    let arrow_msg = arrow_msg.to_application(()).ok()?;
    let chunk = Chunk::from_arrow_msg(&arrow_msg).ok()?;
    chunk.iter_timepoints().last()
}
