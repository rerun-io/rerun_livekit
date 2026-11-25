//! The robot side: reads a Rerun stream from a proxy and publishes it to the room.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use bytes::Bytes;
use futures_util::FutureExt as _;
use livekit::SendByteOptions;
use livekit::participant::ParticipantState;
use livekit::prelude::*;
use prost::Message as _;
use re_protos::log_msg::v1alpha1::LogMsg;
use re_protos::log_msg::v1alpha1::log_msg::Msg;
use re_protos::sdk_comms::v1alpha1::ReadMessagesRequest;
use rerun::external::re_log_encoding::rrd::Compression;
use rerun::external::re_uri::ProxyUri;
use rerun::log::Chunk;
use rerun_livekit::{Role, RoomArgs};
use tokio::sync::mpsc;

use crate::classify::{Retention, Route, StreamKey, Temporal, classify};
use crate::rules::{Delivery, Rules, Video};
use crate::video::{EncoderLimits, Sent, TRACK_INFO_TOPIC, VideoCodecArg, VideoPublisher};
use crate::wire::{LATEST_TRACK, LOG_TOPIC, LatestFrame};

/// The largest message the latest lane can carry.
///
/// LiveKit's receiver gives up on a data track frame after 128 packets of at most 16 kB
/// (the depacketizer and packetizer of `livekit-datatrack` 0.3.0). An operator whose link is
/// slower than the robot's gets less: the server drops a frame that would wait more than
/// 100 ms for it (`rtc.datachannel_data_track_target_latency` in its config).
const MAX_LATEST_BYTES: usize = 1_900_000;

/// The most messages to route before looking at anything else, so that an app that never
/// pauses cannot keep the bridge from serving the room.
const MAX_BURST_MESSAGES: usize = 256;

/// How often to report what was dropped and to remove idle video tracks.
const REPORT_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, clap::Args)]
pub struct RobotArgs {
    /// Rerun proxy to read the stream from: a viewer, or `rerun --serve-grpc`.
    #[arg(long, default_value = "rerun+http://127.0.0.1:9876/proxy")]
    proxy: ProxyUri,

    /// TOML file with rules for what to send and how. Without it, everything is sent.
    #[arg(long)]
    rules: Option<PathBuf>,

    /// Drop latest-wins data older than this, in milliseconds.
    #[arg(long, default_value_t = 1000)]
    max_age_millis: u64,

    /// Send images as video. With `false` they travel unchanged, as data, except where a rule
    /// sets `video = true`.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    video: bool,

    /// Codec of the video tracks that carry images.
    #[arg(long, value_enum, default_value_t = VideoCodecArg::H264)]
    video_codec: VideoCodecArg,

    /// Most bits per second a video track may use. Without it, LiveKit picks a limit by
    /// resolution. The link's congestion control lowers it further.
    #[arg(long)]
    video_max_bitrate_bps: Option<u64>,

    /// Most frames per second a video track may send. Without it, LiveKit picks a limit by
    /// image width: 15 below 320, 20 below 640, 25 below 960, 30 from there up.
    #[arg(long)]
    video_max_framerate_hz: Option<f64>,

    /// How much static data to keep for operators who join later, in bytes.
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    replay_limit_bytes: usize,

    /// Send the driver's commands to this UDP address, as the bytes the driver sent, for a
    /// robot stack that is not written in Rust. An empty datagram means stop.
    #[arg(long)]
    pub commands_to_udp: Option<SocketAddr>,

    #[command(flatten)]
    pub room: RoomArgs,
}

/// Runs one session, which lasts until the proxy or the room goes away.
///
/// # Errors
/// Always, with what ended the session.
pub async fn run_session(args: &RobotArgs) -> anyhow::Result<Infallible> {
    let default_video = Video::from(args.video);
    let rules = match &args.rules {
        Some(path) => Rules::load(path, default_video)?,
        None => Rules::new(default_video),
    };
    let (room, mut events) = args.room.connect(Role::Robot).await?;
    let result = forward(args, &rules, &room, &mut events)
        .await
        .map_err(|err| crate::session_error(err, &mut events));
    room.close().await.ok();
    result
}

async fn forward(
    args: &RobotArgs,
    rules: &Rules,
    room: &Room,
    events: &mut mpsc::UnboundedReceiver<RoomEvent>,
) -> anyhow::Result<Infallible> {
    let mut log_msgs = crate::proxy::connect(&args.proxy)
        .await?
        .read_messages(ReadMessagesRequest {})
        .await
        .context("Failed to subscribe to the Rerun proxy")?
        .into_inner();
    log::info!("Forwarding {} to the room", args.proxy);

    let local = room.local_participant();
    // Unbounded on purpose: guaranteed messages are never dropped, and waiting here would
    // make the proxy stall the app that is logging.
    let (guaranteed_tx, guaranteed_rx) = mpsc::unbounded_channel();
    let mut sender = tokio::spawn(send_guaranteed(local.clone(), guaranteed_rx));
    let latest_track = local
        .publish_data_track(LATEST_TRACK)
        .await
        .context("Failed to publish the data track")?;
    let mut router = Router {
        rules,
        gate: Gate::new(Duration::from_millis(args.max_age_millis)),
        latest: LatestLane::new(latest_track),
        video: VideoPublisher::new(
            local,
            args.video_codec.into(),
            EncoderLimits {
                max_bitrate_bps: args.video_max_bitrate_bps,
                max_framerate_hz: args.video_max_framerate_hz,
            },
        ),
        replay: ReplayCache::new(args.replay_limit_bytes),
        operators: Vec::new(),
        guaranteed_tx,
    };
    // An operator that is already active in the room causes no `ParticipantActive` event.
    for participant in room.remote_participants().into_values() {
        if Role::of(&participant) == Some(Role::Operator)
            && participant.state() == ParticipantState::Active
        {
            router.add_operator(participant.identity());
        }
    }
    let mut report = tokio::time::interval(REPORT_INTERVAL);

    loop {
        tokio::select! {
            response = log_msgs.message() => {
                // An app logs in bursts. Route everything that has arrived, then send the
                // latest-wins part of it as one frame.
                let mut next = Some(response);
                let mut num_routed = 0;
                while let Some(response) = next.take() {
                    let log_msg = response
                        .context("Lost the Rerun proxy")?
                        .context("The Rerun proxy closed the stream")?
                        .log_msg
                        .context("The Rerun proxy sent an empty message")?;
                    router.route(log_msg).await?;
                    num_routed += 1;
                    if num_routed < MAX_BURST_MESSAGES {
                        next = log_msgs.message().now_or_never();
                    }
                }
                router.latest.flush()?;
            }

            event = events.recv() => match event.context("The room closed")? {
                RoomEvent::ParticipantActive(participant)
                    if Role::of(&participant) == Some(Role::Operator) =>
                {
                    router.add_operator(participant.identity());
                }
                RoomEvent::ParticipantDisconnected(participant) => {
                    router.remove_operator(&participant.identity());
                }
                RoomEvent::Disconnected { reason } => {
                    return Err(rerun_livekit::Error::Disconnected { reason }.into());
                }
                _ => {}
            },

            _ = report.tick() => {
                router.gate.report_and_reset();
                router.video.unpublish_idle().await?;
            }

            result = &mut sender => {
                result.context("The send task panicked")??;
                anyhow::bail!("The send task stopped");
            }
        }
    }
}

/// Sends each message from the proxy down the lane it belongs on.
struct Router<'a> {
    rules: &'a Rules,
    gate: Gate,
    latest: LatestLane,
    video: VideoPublisher,
    replay: ReplayCache,

    /// The operators that have been sent the replay.
    ///
    /// Guaranteed messages go to these only, so each operator gets a message once: in its
    /// replay, or as it is sent.
    operators: Vec<ParticipantIdentity>,
    guaranteed_tx: mpsc::UnboundedSender<Guaranteed>,
}

impl Router<'_> {
    async fn route(&mut self, log_msg: LogMsg) -> anyhow::Result<()> {
        let age = chunk_age(&log_msg);
        let encode = || Bytes::from(log_msg.encode_to_vec());

        // A chunk that cannot be read cannot be routed, so it travels as it is.
        let route = classify(&log_msg, self.rules).unwrap_or_else(|err| {
            log::warn!("Forwarding a message without looking inside: {err:#}");
            Route::Guaranteed(Retention::None)
        });
        match route {
            Route::Guaranteed(retention) => {
                self.queue_guaranteed(retention, LOG_TOPIC, encode());
            }
            Route::Temporal(temporal) => self.route_temporal(*temporal, &log_msg, age).await?,
            Route::Drop => {}
        }
        Ok(())
    }

    async fn route_temporal(
        &mut self,
        temporal: Temporal,
        original: &LogMsg,
        age: Option<Duration>,
    ) -> anyhow::Result<()> {
        let Temporal {
            stream,
            max_hz,
            delivery,
            store_id,
            chunk,
            is_whole,
            image_columns,
        } = temporal;

        // Staleness and `max_hz` apply to what may be dropped: latest-wins data and video.
        let is_guaranteed = match delivery {
            Delivery::Guaranteed | Delivery::Latched => true,
            Delivery::Latest => false,
        };
        let is_gated = !is_guaranteed || image_columns.is_some();
        let is_admitted = !is_gated || self.gate.admits(&stream, max_hz, age);
        if !is_admitted && !is_guaranteed {
            return Ok(());
        }

        // The image columns leave the chunk if they went out as video, or were not admitted.
        let mut columns_not_sent_as_data = Vec::new();
        if let Some(image_columns) = image_columns {
            if !is_admitted {
                columns_not_sent_as_data = image_columns;
            } else if let Sent::Video { new_track } =
                self.video.send(&stream, &store_id, &chunk).await?
            {
                if let Some(info) = new_track {
                    self.queue_guaranteed(Retention::Persistent, TRACK_INFO_TOPIC, info);
                }
                columns_not_sent_as_data = image_columns;
            }
        }

        let encode = |chunk: &Chunk| {
            crate::proxy::encode_chunk(store_id.clone(), chunk, Compression::LZ4)
                .map(|log_msg| Bytes::from(log_msg.encode_to_vec()))
        };
        let bytes = if !columns_not_sent_as_data.is_empty() {
            let rest = chunk.components_dropped(&columns_not_sent_as_data);
            if rest.components().is_empty() {
                return Ok(());
            }
            encode(&rest)?
        } else if is_whole {
            Bytes::from(original.encode_to_vec())
        } else {
            encode(&chunk)?
        };
        match delivery {
            Delivery::Guaranteed => self.queue_guaranteed(Retention::None, LOG_TOPIC, bytes),
            Delivery::Latched => self.queue_guaranteed(Retention::Static, LOG_TOPIC, bytes),
            Delivery::Latest => self.latest.send(&stream, bytes)?,
        }
        Ok(())
    }

    fn queue_guaranteed(&mut self, retention: Retention, topic: &'static str, bytes: Bytes) {
        let message = ByteStreamMessage { topic, bytes };
        self.replay.insert(retention, &message);
        self.queue_to(message, self.operators.clone());
    }

    fn queue_to(&self, message: ByteStreamMessage, destinations: Vec<ParticipantIdentity>) {
        // LiveKit takes no destinations to mean everyone in the room.
        if destinations.is_empty() {
            return;
        }
        self.guaranteed_tx
            .send(Guaranteed {
                message,
                destinations,
            })
            .ok();
    }

    /// Sends an operator who just joined what it needs to catch up, and from then on every
    /// guaranteed message.
    fn add_operator(&mut self, identity: ParticipantIdentity) {
        if self.operators.contains(&identity) {
            return;
        }
        log::info!("Replaying {} messages to {identity}", self.replay.len());
        for message in self.replay.iter() {
            self.queue_to(message, vec![identity.clone()]);
        }
        self.operators.push(identity);
    }

    fn remove_operator(&mut self, identity: &ParticipantIdentity) {
        self.operators.retain(|operator| operator != identity);
    }
}

/// How long ago the SDK created the chunk in `log_msg`.
///
/// `None` if the message has no chunk id or the clocks disagree, in which case it is not stale.
fn chunk_age(log_msg: &LogMsg) -> Option<Duration> {
    let Some(Msg::ArrowMsg(arrow_msg)) = &log_msg.msg else {
        return None;
    };
    let created_nanos = arrow_msg.chunk_id.as_ref()?.time_ns?;
    let created = UNIX_EPOCH + Duration::from_nanos(created_nanos);
    SystemTime::now().duration_since(created).ok()
}

// ----------------------------------------------------------------------------

/// An encoded message and the byte stream topic that says what it is.
#[derive(Clone)]
struct ByteStreamMessage {
    topic: &'static str,
    bytes: Bytes,
}

/// A message that must arrive, in order.
struct Guaranteed {
    message: ByteStreamMessage,

    /// Never empty.
    destinations: Vec<ParticipantIdentity>,
}

async fn send_guaranteed(
    local: LocalParticipant,
    mut guaranteed_rx: mpsc::UnboundedReceiver<Guaranteed>,
) -> anyhow::Result<()> {
    while let Some(Guaranteed {
        message,
        destinations,
    }) = guaranteed_rx.recv().await
    {
        let ByteStreamMessage { topic, bytes } = message;
        // Rerun already compresses chunks where that helps.
        let mut options = SendByteOptions::new_with_topic(topic).with_compress(false);
        options.destination_identities = destinations;
        local
            .send_bytes(&bytes, options)
            .await
            .context("Failed to send a message to the room")?;
    }
    Ok(())
}

// ----------------------------------------------------------------------------

/// Decides which latest-wins messages, data and video frames alike, are worth sending.
struct Gate {
    max_age: Duration,
    last_sent: ahash::HashMap<StreamKey, Instant>,
    num_stale: u64,
    num_rate_limited: u64,
}

impl Gate {
    fn new(max_age: Duration) -> Self {
        Self {
            max_age,
            last_sent: Default::default(),
            num_stale: 0,
            num_rate_limited: 0,
        }
    }

    /// Whether a message of `stream` that is `age` old is recent enough and within `max_hz`.
    fn admits(&mut self, stream: &StreamKey, max_hz: Option<f64>, age: Option<Duration>) -> bool {
        if age.is_some_and(|age| self.max_age < age) {
            self.num_stale += 1;
            return false;
        }
        let Some(max_hz) = max_hz else {
            return true;
        };

        let now = Instant::now();
        let min_interval = Duration::from_secs_f64(1.0 / max_hz);
        let is_too_soon = self
            .last_sent
            .get(stream)
            .is_some_and(|last_sent| now.duration_since(*last_sent) < min_interval);
        if is_too_soon {
            self.num_rate_limited += 1;
            return false;
        }
        self.last_sent.insert(stream.clone(), now);
        true
    }

    fn report_and_reset(&mut self) {
        let num_stale = std::mem::take(&mut self.num_stale);
        let num_rate_limited = std::mem::take(&mut self.num_rate_limited);
        if 0 < num_stale + num_rate_limited {
            log::info!(
                "Latest-wins messages not sent: {num_stale} stale, {num_rate_limited} over `max_hz`"
            );
        }
    }
}

/// Sends latest-wins messages on a lossy data track, a burst at a time.
///
/// LiveKit keeps only the newest unsent data track frame of a participant, so of two frames
/// pushed back to back the first can be lost. Sending the messages that were logged together
/// as one frame keeps them from evicting each other.
struct LatestLane {
    track: LocalDataTrack,

    /// Messages waiting to go out as one frame.
    pending: LatestFrame,
    pending_bytes: usize,
}

impl LatestLane {
    fn new(track: LocalDataTrack) -> Self {
        Self {
            track,
            pending: LatestFrame::default(),
            pending_bytes: 0,
        }
    }

    /// Adds a message to the frame that the next [`Self::flush`] sends.
    fn send(&mut self, stream: &StreamKey, bytes: Bytes) -> anyhow::Result<()> {
        if MAX_LATEST_BYTES < bytes.len() {
            log::warn!(
                "Dropping a {} byte message of {stream}: too large for the lossy lane. \
                 Reduce it at the source, or give it `delivery = \"guaranteed\"` in the rules",
                bytes.len()
            );
            return Ok(());
        }
        if MAX_LATEST_BYTES < self.pending_bytes + bytes.len() {
            self.flush()?;
        }
        self.pending_bytes += bytes.len();
        self.pending.log_msgs.push(bytes);
        Ok(())
    }

    /// Sends what [`Self::send`] collected, as one frame.
    fn flush(&mut self) -> anyhow::Result<()> {
        if self.pending.log_msgs.is_empty() {
            return Ok(());
        }
        let frame = std::mem::take(&mut self.pending).encode_to_vec();
        self.pending_bytes = 0;
        match self.track.try_push(DataTrackFrame::new(frame)) {
            Ok(()) => Ok(()),
            Err(err) => match err.reason() {
                // The track is being published again after a reconnect, or LiveKit's pipeline
                // fell behind. The next frame replaces this one either way.
                PushFrameErrorReason::QueueFull => Ok(()),
                PushFrameErrorReason::TrackUnpublished => {
                    anyhow::bail!("The latest-wins data track was unpublished")
                }
            },
        }
    }
}

// ----------------------------------------------------------------------------

/// What an operator who joins late needs to see the same scene as everyone else.
///
/// LiveKit replays nothing to late joiners, so this mirrors what Rerun's own proxy keeps for
/// late viewers: store info and blueprints forever, and static data up to a memory limit.
struct ReplayCache {
    persistent: Vec<ByteStreamMessage>,
    static_data: VecDeque<ByteStreamMessage>,
    static_data_bytes: usize,
    static_limit_bytes: usize,

    /// Whether static data was ever dropped to stay within the limit.
    has_evicted: bool,
}

impl ReplayCache {
    fn new(static_limit_bytes: usize) -> Self {
        Self {
            persistent: Vec::new(),
            static_data: VecDeque::new(),
            static_data_bytes: 0,
            static_limit_bytes,
            has_evicted: false,
        }
    }

    fn insert(&mut self, retention: Retention, message: &ByteStreamMessage) {
        match retention {
            Retention::Persistent => self.persistent.push(message.clone()),
            Retention::Static => {
                self.static_data_bytes += message.bytes.len();
                self.static_data.push_back(message.clone());
                while self.static_limit_bytes < self.static_data_bytes {
                    let Some(evicted) = self.static_data.pop_front() else {
                        break;
                    };
                    self.static_data_bytes -= evicted.bytes.len();
                    if !self.has_evicted {
                        self.has_evicted = true;
                        log::warn!(
                            "The static data no longer fits in `--replay-limit-bytes` ({} bytes). \
                             Operators who join from now on will miss the oldest of it",
                            self.static_limit_bytes
                        );
                    }
                }
            }
            Retention::None => {}
        }
    }

    fn len(&self) -> usize {
        self.persistent.len() + self.static_data.len()
    }

    /// Store info and blueprints first, so the viewer knows the stores before their data.
    fn iter(&self) -> impl Iterator<Item = ByteStreamMessage> {
        itertools::chain!(&self.persistent, &self.static_data).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(num_bytes: usize) -> ByteStreamMessage {
        ByteStreamMessage {
            topic: LOG_TOPIC,
            bytes: Bytes::from(vec![0; num_bytes]),
        }
    }

    #[test]
    fn replay_evicts_the_oldest_static_data_only() {
        let mut replay = ReplayCache::new(100);
        replay.insert(Retention::Persistent, &message(1000));
        replay.insert(Retention::None, &message(1000));
        for num_bytes in [40, 41, 42] {
            replay.insert(Retention::Static, &message(num_bytes));
        }

        let sizes: Vec<_> = replay.iter().map(|message| message.bytes.len()).collect();
        assert_eq!(sizes, [1000, 41, 42]);
        assert!(replay.has_evicted);
    }

    #[test]
    fn gate_drops_stale_and_too_frequent_messages() {
        let mut gate = Gate::new(Duration::from_secs(1));
        let (camera, lidar) = (
            StreamKey::for_test("/camera"),
            StreamKey::for_test("/lidar"),
        );
        let fresh = Some(Duration::from_millis(10));

        assert!(!gate.admits(&camera, None, Some(Duration::from_secs(2))));
        assert!(
            gate.admits(&camera, None, None),
            "an unknown age is not stale"
        );

        assert!(gate.admits(&camera, Some(1.0), fresh));
        assert!(
            !gate.admits(&camera, Some(1.0), fresh),
            "second message within a second"
        );
        assert!(
            gate.admits(&lidar, Some(1.0), fresh),
            "each stream has its own rate"
        );

        assert_eq!((gate.num_stale, gate.num_rate_limited), (1, 1));
    }
}
