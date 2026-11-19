//! Carries images over LiveKit video tracks.
//!
//! The robot side turns each image row into a video frame on one track per image stream.
//! The operator side turns each decoded frame back into a row of the same archetype, at the
//! same entity path, with the same row id and times. Pixels are lossy; nothing else changes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use bytes::Bytes;
use futures_util::StreamExt as _;
use image::ImageEncoder as _;
use itertools::izip;
use livekit::options::{
    FrameMetadataFeatures, TrackPublishOptions, VideoCodec, VideoEncoding,
    compute_appropriate_encoding,
};
use livekit::prelude::*;
use livekit::webrtc::native::yuv_helper;
use livekit::webrtc::video_frame::{
    FrameMetadata, I420Buffer, VideoBuffer as _, VideoFrame, VideoRotation,
};
use livekit::webrtc::video_source::native::NativeVideoSource;
use livekit::webrtc::video_source::{RtcVideoSource, VideoResolution};
use livekit::webrtc::video_stream::native::NativeVideoStream;
use prost::Message as _;
use re_protos::common::v1alpha1::StoreId as StoreIdProto;
use re_protos::log_msg::v1alpha1::LogMsg;
use re_protos::sdk_comms::v1alpha1::WriteMessagesRequest;
use rerun::external::re_log_encoding::rrd::Compression;
use rerun::external::re_log_types::TimeType;
use rerun::log::{Chunk, RowId, TimeColumn};
use rerun::{
    ChannelDatatype, ColorModel, ComponentIdentifier, EntityPath, TimePoint, Timeline,
    TimelineName, archetypes, components, datatypes,
};
use tokio::sync::mpsc;

use crate::classify::StreamKey;
use crate::latency::{LatencyPlots, LatencyReport};

/// Topic of the byte streams that announce video tracks with a [`TrackInfo`].
pub const TRACK_INFO_TOPIC: &str = "rerun.video";

/// Video tracks that carry Rerun images are named with this prefix and a number.
const TRACK_NAME_PREFIX: &str = "rerun.video.";

/// How many timelines fit in a frame's metadata next to the row id.
///
/// LiveKit drops frame metadata over about 232 bytes (`FrameMetadata::user_data` in
/// `libwebrtc` 0.4.1). A row id takes 16 and a time takes 8.
const MAX_TIMELINES: usize = 16;

/// A track without frames for this long is unpublished.
///
/// An app that restarts logs to a new recording, which gets new tracks. Without this the
/// tracks of every past run would stay in the room.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Quality of the JPEGs the operator side makes for `EncodedImage`.
///
/// High, because the viewer is on the same machine and the lossy step already happened.
const JPEG_QUALITY: u8 = 90;

/// The codecs LiveKit can encode images with.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum VideoCodecArg {
    Av1,
    H264,
    H265,
    Vp8,
    Vp9,
}

impl From<VideoCodecArg> for VideoCodec {
    fn from(codec: VideoCodecArg) -> Self {
        match codec {
            VideoCodecArg::Av1 => Self::AV1,
            VideoCodecArg::H264 => Self::H264,
            VideoCodecArg::H265 => Self::H265,
            VideoCodecArg::Vp8 => Self::VP8,
            VideoCodecArg::Vp9 => Self::VP9,
        }
    }
}

// ----------------------------------------------------------------------------
// Wire format

/// Everything about a video track's frames that is the same for every frame.
///
/// A track never changes its info. When an image stream changes, the robot starts a new track.
#[derive(Clone, PartialEq, prost::Message)]
pub struct TrackInfo {
    #[prost(string, tag = "1")]
    track_name: String,

    #[prost(message, optional, tag = "2")]
    store_id: Option<StoreIdProto>,

    #[prost(string, tag = "3")]
    entity_path: String,

    #[prost(uint32, tag = "4")]
    width: u32,

    #[prost(uint32, tag = "5")]
    height: u32,

    #[prost(enumeration = "Layout", tag = "6")]
    layout: i32,

    /// Set for `EncodedImage`, unset for `Image`.
    #[prost(message, optional, tag = "7")]
    encoded: Option<Encoded>,

    /// The timelines of every frame, in the order their times appear in the frame metadata.
    #[prost(message, repeated, tag = "8")]
    timelines: Vec<TimelineInfo>,
}

/// How the pixels of an 8-bit image are laid out.
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum Layout {
    L = 0,
    Rgb = 1,
    Rgba = 2,
    Bgr = 3,
    Bgra = 4,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Encoded {
    #[prost(enumeration = "Codec", tag = "1")]
    codec: i32,

    /// The `media_type` component of the source rows, if they had one.
    #[prost(string, optional, tag = "2")]
    media_type: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum Codec {
    Jpeg = 0,
    Png = 1,
}

#[derive(Clone, PartialEq, prost::Message)]
struct TimelineInfo {
    #[prost(string, tag = "1")]
    name: String,

    #[prost(enumeration = "TimeKind", tag = "2")]
    kind: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum TimeKind {
    Sequence = 0,
    DurationNs = 1,
    TimestampNs = 2,
}

impl From<TimeType> for TimeKind {
    fn from(time_type: TimeType) -> Self {
        match time_type {
            TimeType::Sequence => Self::Sequence,
            TimeType::DurationNs => Self::DurationNs,
            TimeType::TimestampNs => Self::TimestampNs,
        }
    }
}

impl From<TimeKind> for TimeType {
    fn from(kind: TimeKind) -> Self {
        match kind {
            TimeKind::Sequence => Self::Sequence,
            TimeKind::DurationNs => Self::DurationNs,
            TimeKind::TimestampNs => Self::TimestampNs,
        }
    }
}

/// What rides along with each frame: the row id, then one time per timeline of the track.
fn encode_frame_metadata(row_id: RowId, times: impl Iterator<Item = i64>) -> Vec<u8> {
    let mut user_data = row_id.as_u128().to_le_bytes().to_vec();
    for time in times {
        user_data.extend_from_slice(&time.to_le_bytes());
    }
    user_data
}

fn decode_frame_metadata(user_data: &[u8], info: &TrackInfo) -> Option<(RowId, TimePoint)> {
    let (row_id, times) = user_data.split_first_chunk::<16>()?;
    let (times, []) = times.as_chunks::<8>() else {
        return None;
    };
    if times.len() != info.timelines.len() {
        return None;
    }
    let timepoint = izip!(&info.timelines, times)
        .map(|(timeline, time)| {
            let name = TimelineName::try_new(&timeline.name).ok()?;
            Some((
                Timeline::new(name, timeline.kind().into()),
                i64::from_le_bytes(*time),
            ))
        })
        .collect::<Option<_>>()?;
    Some((RowId::from_u128(u128::from_le_bytes(*row_id)), timepoint))
}

// ----------------------------------------------------------------------------
// Pixels

impl Layout {
    fn num_channels(self) -> usize {
        match self {
            Self::L => 1,
            Self::Rgb | Self::Bgr => 3,
            Self::Rgba | Self::Bgra => 4,
        }
    }

    fn from_color_model(color_model: ColorModel) -> Self {
        match color_model {
            ColorModel::L => Self::L,
            ColorModel::RGB => Self::Rgb,
            ColorModel::RGBA => Self::Rgba,
            ColorModel::BGR => Self::Bgr,
            ColorModel::BGRA => Self::Bgra,
        }
    }

    fn color_model(self) -> ColorModel {
        match self {
            Self::L => ColorModel::L,
            Self::Rgb => ColorModel::RGB,
            Self::Rgba => ColorModel::RGBA,
            Self::Bgr => ColorModel::BGR,
            Self::Bgra => ColorModel::BGRA,
        }
    }

    fn rgba_from_pixels(self, pixels: &[u8]) -> Vec<u8> {
        match self {
            Self::L => pixels.iter().flat_map(|&l| [l, l, l, 255]).collect(),
            Self::Rgb => {
                let (pixels, _) = pixels.as_chunks::<3>();
                pixels
                    .iter()
                    .flat_map(|&[r, g, b]| [r, g, b, 255])
                    .collect()
            }
            Self::Rgba => pixels.to_vec(),
            Self::Bgr => {
                let (pixels, _) = pixels.as_chunks::<3>();
                pixels
                    .iter()
                    .flat_map(|&[b, g, r]| [r, g, b, 255])
                    .collect()
            }
            Self::Bgra => {
                let (pixels, _) = pixels.as_chunks::<4>();
                pixels
                    .iter()
                    .flat_map(|&[b, g, r, a]| [r, g, b, a])
                    .collect()
            }
        }
    }

    /// The inverse of [`Self::rgba_from_pixels`]. Video has no alpha, so alpha comes back opaque.
    fn pixels_from_rgba(self, rgba: &[u8]) -> Vec<u8> {
        let (rgba, _) = rgba.as_chunks::<4>();
        match self {
            Self::L => rgba.iter().map(|&[r, _, _, _]| r).collect(),
            Self::Rgb => rgba.iter().flat_map(|&[r, g, b, _]| [r, g, b]).collect(),
            Self::Rgba => rgba
                .iter()
                .flat_map(|&[r, g, b, _]| [r, g, b, 255])
                .collect(),
            Self::Bgr => rgba.iter().flat_map(|&[r, g, b, _]| [b, g, r]).collect(),
            Self::Bgra => rgba
                .iter()
                .flat_map(|&[r, g, b, _]| [b, g, r, 255])
                .collect(),
        }
    }
}

fn rgba_to_i420(rgba: &[u8], width: u32, height: u32) -> I420Buffer {
    let mut i420 = I420Buffer::new(width, height);
    let (stride_y, stride_u, stride_v) = i420.strides();
    let (y, u, v) = i420.data_mut();
    // libyuv names formats by their little-endian word, so its "ABGR" is R, G, B, A in memory.
    yuv_helper::abgr_to_i420(
        rgba,
        width * 4,
        y,
        stride_y,
        u,
        stride_u,
        v,
        stride_v,
        dimension(width),
        dimension(height),
    );
    i420
}

fn i420_to_rgba(i420: &I420Buffer) -> Vec<u8> {
    let (width, height) = (i420.width(), i420.height());
    let mut rgba = vec![0; width as usize * height as usize * 4];
    let (stride_y, stride_u, stride_v) = i420.strides();
    let (y, u, v) = i420.data();
    yuv_helper::i420_to_abgr(
        y,
        stride_y,
        u,
        stride_u,
        v,
        stride_v,
        &mut rgba,
        width * 4,
        dimension(width),
        dimension(height),
    );
    rgba
}

/// Converts an image width or height to what libyuv takes.
fn dimension(pixels: u32) -> i32 {
    i32::try_from(pixels).expect("an image this large does not fit in memory")
}

// ----------------------------------------------------------------------------
// Robot side

/// The columns of `chunk` that can travel as video, if it has images.
///
/// The chunk's other columns (a coordinate frame, an opacity, the original ROS message)
/// are not part of the frames and have to travel as data.
pub fn image_columns(chunk: &Chunk) -> Option<Vec<ComponentIdentifier>> {
    if MAX_TIMELINES < chunk.timelines().len() {
        return None;
    }
    let has = |component: &ComponentIdentifier| chunk.components().contains_key(component);

    let image = [
        archetypes::Image::descriptor_buffer().component,
        archetypes::Image::descriptor_format().component,
    ];
    if image.iter().all(has) {
        return Some(image.to_vec());
    }

    let blob = archetypes::EncodedImage::descriptor_blob().component;
    let media_type = archetypes::EncodedImage::descriptor_media_type().component;
    has(&blob).then(|| [blob, media_type].into_iter().filter(has).collect())
}

/// One image row, ready to become a video frame.
struct Frame {
    width: u32,
    height: u32,
    layout: Layout,
    encoded: Option<Encoded>,
    rgba: Vec<u8>,
}

impl Frame {
    /// Reads row `row` of `chunk`. `None` if the image is not 8-bit or otherwise unsupported.
    fn from_row(chunk: &Chunk, row: usize) -> Option<Self> {
        let unit = chunk.row_sliced_unit_shallow(row);

        let buffer = archetypes::Image::descriptor_buffer().component;
        if let Some(buffer) = unit.component_mono::<components::ImageBuffer>(buffer) {
            let format = archetypes::Image::descriptor_format().component;
            let format = unit
                .component_mono::<components::ImageFormat>(format)?
                .ok()?
                .0;
            let datatypes::ImageFormat {
                width,
                height,
                pixel_format: None,
                color_model: Some(color_model),
                channel_datatype: Some(ChannelDatatype::U8),
            } = format
            else {
                return None;
            };
            let layout = Layout::from_color_model(color_model);
            let buffer = buffer.ok()?;
            let pixels: &[u8] = &buffer.0;
            if pixels.len() != width as usize * height as usize * layout.num_channels() {
                return None;
            }
            return Some(Self {
                width,
                height,
                layout,
                encoded: None,
                rgba: layout.rgba_from_pixels(pixels),
            });
        }

        let blob = archetypes::EncodedImage::descriptor_blob().component;
        let blob = unit.component_mono::<components::Blob>(blob)?.ok()?;
        let media_type = archetypes::EncodedImage::descriptor_media_type().component;
        let media_type = match unit.component_mono::<components::MediaType>(media_type) {
            Some(media_type) => Some(media_type.ok()?.to_string()),
            None => None,
        };

        let codec = match image::guess_format(&blob).ok()? {
            image::ImageFormat::Jpeg => Codec::Jpeg,
            image::ImageFormat::Png => Codec::Png,
            _ => return None,
        };
        let decoded = image::load_from_memory(&blob).ok()?;
        let layout = match decoded.color() {
            image::ColorType::L8 => Layout::L,
            image::ColorType::Rgb8 => Layout::Rgb,
            image::ColorType::Rgba8 => Layout::Rgba,
            _ => return None,
        };
        Some(Self {
            width: decoded.width(),
            height: decoded.height(),
            layout,
            encoded: Some(Encoded {
                codec: codec.into(),
                media_type,
            }),
            rgba: decoded.into_rgba8().into_raw(),
        })
    }
}

/// What became of a chunk given to [`VideoPublisher::send`].
pub enum Sent {
    /// The chunk went out as video frames.
    Video {
        /// An encoded [`TrackInfo`] to deliver, guaranteed, on [`TRACK_INFO_TOPIC`],
        /// if these frames started a new track.
        new_track: Option<Bytes>,
    },

    /// The chunk holds an image that video cannot carry. Send it some other way.
    Unsupported,
}

/// Limits for the encoder of each video track. Unset ones keep LiveKit's preset.
///
/// LiveKit picks a preset by resolution (`compute_appropriate_encoding` in `livekit` 0.11.0):
/// from 15 frames per second for images under 320 wide to 30 from 960 wide. The link's
/// congestion control lowers both limits further.
#[derive(Clone, Copy, Debug, Default)]
pub struct EncoderLimits {
    pub max_bitrate_bps: Option<u64>,
    pub max_framerate_hz: Option<f64>,
}

impl EncoderLimits {
    /// The encoding for a track, or `None` to let LiveKit choose it.
    fn encoding(self, width: u32, height: u32, codec: VideoCodec) -> Option<VideoEncoding> {
        let Self {
            max_bitrate_bps,
            max_framerate_hz,
        } = self;
        if max_bitrate_bps.is_none() && max_framerate_hz.is_none() {
            return None;
        }

        let preset = compute_appropriate_encoding(false, width, height, codec);
        Some(VideoEncoding {
            max_bitrate: max_bitrate_bps.unwrap_or(preset.max_bitrate),
            max_framerate: max_framerate_hz.unwrap_or(preset.max_framerate),
        })
    }
}

/// Publishes one video track per image stream.
pub struct VideoPublisher {
    local: LocalParticipant,
    codec: VideoCodec,
    limits: EncoderLimits,
    tracks: ahash::HashMap<StreamKey, PublishedTrack>,
    num_tracks_started: u64,
}

struct PublishedTrack {
    info: TrackInfo,
    source: NativeVideoSource,
    publication: LocalTrackPublication,
    last_frame: Instant,
}

impl VideoPublisher {
    pub fn new(local: LocalParticipant, codec: VideoCodec, limits: EncoderLimits) -> Self {
        Self {
            local,
            codec,
            limits,
            tracks: Default::default(),
            num_tracks_started: 0,
        }
    }

    /// Sends the image rows of `chunk`, which must have [`image_columns`], as video frames.
    pub async fn send(
        &mut self,
        stream: &StreamKey,
        store_id: &StoreIdProto,
        chunk: &Chunk,
    ) -> anyhow::Result<Sent> {
        let Some(frames) = (0..chunk.num_rows())
            .map(|row| Frame::from_row(chunk, row))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(Sent::Unsupported);
        };

        let columns = time_columns_by_name(chunk);
        let stream_info = StreamInfo {
            store_id,
            entity_path: chunk.entity_path().to_string(),
            timelines: columns
                .iter()
                .map(|column| TimelineInfo {
                    name: column.name().to_owned(),
                    kind: TimeKind::from(column.timeline().typ()).into(),
                })
                .collect(),
        };

        let mut new_track = None;
        for (row, row_id, frame) in izip!(0.., chunk.row_ids(), &frames) {
            let track = match self.tracks.get_mut(stream) {
                Some(track) if track.info.carries(&stream_info, frame) => track,
                _ => {
                    let track = self.start_track(stream, &stream_info, frame).await?;
                    new_track = Some(Bytes::from(track.info.encode_to_vec()));
                    track
                }
            };
            track.last_frame = Instant::now();

            let times = columns.iter().map(|column| column.times_raw()[row]);
            track.source.capture_frame(&VideoFrame {
                rotation: VideoRotation::VideoRotation0,
                timestamp_us: 0, // now
                frame_metadata: Some(FrameMetadata {
                    user_timestamp: None,
                    frame_id: None,
                    user_data: Some(encode_frame_metadata(row_id, times)),
                }),
                buffer: rgba_to_i420(&frame.rgba, frame.width, frame.height),
            });
        }
        Ok(Sent::Video { new_track })
    }

    /// Publishes a new track for `stream`, replacing the one it had.
    ///
    /// This waits for the room to accept the track, which holds up the caller once per track.
    async fn start_track(
        &mut self,
        stream: &StreamKey,
        stream_info: &StreamInfo<'_>,
        frame: &Frame,
    ) -> anyhow::Result<&mut PublishedTrack> {
        if let Some(replaced) = self.tracks.remove(stream) {
            self.unpublish(replaced).await?;
        }

        let StreamInfo {
            store_id,
            entity_path,
            timelines,
        } = stream_info;
        let info = TrackInfo {
            track_name: format!("{TRACK_NAME_PREFIX}{}", self.num_tracks_started),
            store_id: Some((*store_id).clone()),
            entity_path: entity_path.clone(),
            width: frame.width,
            height: frame.height,
            layout: frame.layout.into(),
            encoded: frame.encoded.clone(),
            timelines: timelines.clone(),
        };
        self.num_tracks_started += 1;

        let resolution = VideoResolution {
            width: info.width,
            height: info.height,
        };
        let source = NativeVideoSource::new(resolution, false);
        let track = LocalVideoTrack::create_video_track(
            &info.track_name,
            RtcVideoSource::Native(source.clone()),
        );
        let mut frame_metadata_features = FrameMetadataFeatures::default();
        frame_metadata_features.user_data = true;
        let options = TrackPublishOptions {
            source: TrackSource::Camera,
            video_codec: self.codec,
            video_encoding: self.limits.encoding(info.width, info.height, self.codec),
            frame_metadata_features,
            ..Default::default()
        };
        let publication = self
            .local
            .publish_track(LocalTrack::Video(track), options)
            .await
            .with_context(|| format!("Failed to publish a video track for {stream}"))?;
        log::info!(
            "Sending {stream} as video track {:?} ({}x{})",
            info.track_name,
            info.width,
            info.height
        );

        let track = PublishedTrack {
            info,
            source,
            publication,
            last_frame: Instant::now(),
        };
        Ok(self
            .tracks
            .entry(stream.clone())
            .insert_entry(track)
            .into_mut())
    }

    /// Unpublishes the tracks whose image stream has stopped.
    pub async fn unpublish_idle(&mut self) -> anyhow::Result<()> {
        let idle: Vec<_> = self
            .tracks
            .iter()
            .filter(|(_, track)| IDLE_TIMEOUT < track.last_frame.elapsed())
            .map(|(stream, _)| stream.clone())
            .collect();
        for stream in idle {
            if let Some(track) = self.tracks.remove(&stream) {
                log::info!(
                    "{stream} stopped, removing video track {:?}",
                    track.info.track_name
                );
                self.unpublish(track).await?;
            }
        }
        Ok(())
    }

    async fn unpublish(&self, track: PublishedTrack) -> anyhow::Result<()> {
        self.local
            .unpublish_track(&track.publication.sid())
            .await
            .context("Failed to unpublish a video track")?;
        Ok(())
    }
}

/// The time columns of `chunk` in a fixed order.
///
/// The map they come from has no order to rely on, and both a track's info and each frame's
/// metadata list the timelines in this order.
fn time_columns_by_name(chunk: &Chunk) -> Vec<&TimeColumn> {
    let mut columns: Vec<_> = chunk.timelines().values().collect();
    columns.sort_by(|a, b| a.name().cmp(b.name()));
    columns
}

/// What every frame of one chunk has in common.
struct StreamInfo<'a> {
    store_id: &'a StoreIdProto,
    entity_path: String,

    /// Sorted by name.
    timelines: Vec<TimelineInfo>,
}

impl TrackInfo {
    /// Whether `frame` of a chunk described by `stream_info` can go on this track.
    fn carries(&self, stream_info: &StreamInfo<'_>, frame: &Frame) -> bool {
        let Self {
            track_name: _, // names the track, says nothing about its frames
            store_id,
            entity_path,
            width,
            height,
            layout,
            encoded,
            timelines,
        } = self;
        store_id.as_ref() == Some(stream_info.store_id)
            && *entity_path == stream_info.entity_path
            && *timelines == stream_info.timelines
            && *width == frame.width
            && *height == frame.height
            && *layout == i32::from(frame.layout)
            && *encoded == frame.encoded
    }
}

// ----------------------------------------------------------------------------
// Operator side

/// The video tracks the robot has announced, by track name.
#[derive(Clone, Default)]
pub struct TrackInfos(Arc<parking_lot::Mutex<ahash::HashMap<String, Arc<TrackInfo>>>>);

impl TrackInfos {
    /// Records an encoded [`TrackInfo`] received on [`TRACK_INFO_TOPIC`].
    pub fn insert(&self, bytes: Bytes) -> anyhow::Result<()> {
        let info = TrackInfo::decode(bytes).context("Received a malformed video track info")?;
        self.0
            .lock()
            .insert(info.track_name.clone(), Arc::new(info));
        Ok(())
    }
}

/// Whether the video track called `track_name` carries Rerun images.
pub fn is_rerun_track(track_name: &str) -> bool {
    track_name.starts_with(TRACK_NAME_PREFIX)
}

/// Turns the frames of a video track back into log messages, until the track or the viewer goes away.
pub async fn receive_frames(
    track: RemoteVideoTrack,
    track_name: String,
    infos: TrackInfos,
    viewer_tx: mpsc::Sender<WriteMessagesRequest>,
    latency_plots: LatencyPlots,
) {
    let mut frames = NativeVideoStream::new(track.rtc_track());
    let mut latency = None;
    while let Some(frame) = frames.next().await {
        // The info travels on another channel and may arrive after the first frames.
        let Some(info) = infos.0.lock().get(&track_name).cloned() else {
            continue;
        };
        let Some(user_data) = frame.frame_metadata.and_then(|metadata| metadata.user_data) else {
            continue;
        };
        let log_msg = match to_log_msg(&info, &user_data, frame.buffer.to_i420()) {
            Ok(Rebuilt {
                row_id,
                timepoint,
                log_msg,
            }) => {
                if let Some(store_id) = &info.store_id {
                    latency
                        .get_or_insert_with(|| {
                            let entity_path = EntityPath::parse_forgiving(&info.entity_path);
                            let series = EntityPath::from("video").join(&entity_path);
                            LatencyReport::new(format!("Video of {entity_path}")).with_plots(
                                latency_plots,
                                &series,
                                &viewer_tx,
                            )
                        })
                        .record(row_id.nanos_since_epoch(), store_id, || Some(timepoint));
                }
                log_msg
            }
            Err(err) => {
                log::warn!("Dropping a frame of video track {track_name:?}: {err:#}");
                continue;
            }
        };
        let request = WriteMessagesRequest {
            log_msg: Some(log_msg),
        };
        if viewer_tx.send(request).await.is_err() {
            return;
        }
    }
}

/// An image row rebuilt from a video frame.
struct Rebuilt {
    row_id: RowId,
    timepoint: TimePoint,
    log_msg: LogMsg,
}

fn to_log_msg(info: &TrackInfo, user_data: &[u8], mut i420: I420Buffer) -> anyhow::Result<Rebuilt> {
    let (row_id, timepoint) = decode_frame_metadata(user_data, info)
        .context("The frame metadata does not match the track")?;

    // WebRTC lowers the resolution when the link is slow. The image must keep the size the
    // robot logged, or it no longer lines up with its pinhole and annotations.
    if (i420.width(), i420.height()) != (info.width, info.height) {
        i420 = i420.scale(dimension(info.width), dimension(info.height));
    }
    let layout = info.layout();
    let pixels = layout.pixels_from_rgba(&i420_to_rgba(&i420));

    let chunk = Chunk::builder(EntityPath::parse_forgiving(&info.entity_path));
    let chunk = match &info.encoded {
        None => {
            let format = datatypes::ImageFormat::from_color_model(
                [info.width, info.height],
                layout.color_model(),
                ChannelDatatype::U8,
            );
            chunk.with_archetype(
                row_id,
                timepoint.clone(),
                &archetypes::Image::new(pixels, format),
            )
        }
        Some(encoded) => {
            let color = match layout {
                Layout::L => image::ExtendedColorType::L8,
                Layout::Rgb => image::ExtendedColorType::Rgb8,
                Layout::Rgba => image::ExtendedColorType::Rgba8,
                Layout::Bgr | Layout::Bgra => anyhow::bail!("Encoded images are never BGR"),
            };
            let mut blob = Vec::new();
            match encoded.codec() {
                Codec::Jpeg => {
                    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut blob, JPEG_QUALITY)
                        .write_image(&pixels, info.width, info.height, color)
                }
                Codec::Png => image::codecs::png::PngEncoder::new(&mut blob).write_image(
                    &pixels,
                    info.width,
                    info.height,
                    color,
                ),
            }
            .context("Failed to encode the image")?;

            let mut image = archetypes::EncodedImage::new(blob);
            if let Some(media_type) = &encoded.media_type {
                image = image.with_media_type(media_type.as_str());
            }
            chunk.with_archetype(row_id, timepoint.clone(), &image)
        }
    };
    let chunk = chunk.build().context("Failed to build a chunk")?;
    let store_id = info.store_id.clone().context("The track has no store id")?;
    Ok(Rebuilt {
        row_id,
        timepoint,
        // The viewer is next door, so compressing would only cost time.
        log_msg: crate::proxy::encode_chunk(store_id, &chunk, Compression::Off)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_metadata_round_trips() {
        let info = TrackInfo {
            timelines: vec![
                TimelineInfo {
                    name: "log_time".to_owned(),
                    kind: TimeKind::TimestampNs.into(),
                },
                TimelineInfo {
                    name: "step".to_owned(),
                    kind: TimeKind::Sequence.into(),
                },
            ],
            ..Default::default()
        };
        let row_id = RowId::new();
        let user_data = encode_frame_metadata(row_id, [1_700_000_000_000_000_000, 42].into_iter());

        let (decoded_row_id, timepoint) = decode_frame_metadata(&user_data, &info).unwrap();
        assert_eq!(decoded_row_id, row_id);
        let expected = TimePoint::STATIC
            .with(
                Timeline::new("log_time", TimeType::TimestampNs),
                1_700_000_000_000_000_000,
            )
            .with(Timeline::new_sequence("step"), 42);
        assert_eq!(timepoint, expected);

        assert!(decode_frame_metadata(&user_data[..user_data.len() - 8], &info).is_none());
    }

    #[test]
    fn time_columns_are_ordered_by_name() {
        let names = ["step", "log_time", "a_tick", "sim_time"];
        let timepoint: TimePoint = names
            .iter()
            .map(|name| (Timeline::new_sequence(*name), 1))
            .collect();
        let chunk = Chunk::builder("camera")
            .with_archetype(RowId::new(), timepoint, &rerun::Scalars::single(1.0))
            .build()
            .unwrap();

        let ordered: Vec<_> = time_columns_by_name(&chunk)
            .iter()
            .map(|column| column.name().to_owned())
            .collect();
        assert_eq!(ordered, ["a_tick", "log_time", "sim_time", "step"]);
    }

    #[test]
    fn layouts_round_trip_through_rgba() {
        for layout in [
            Layout::L,
            Layout::Rgb,
            Layout::Rgba,
            Layout::Bgr,
            Layout::Bgra,
        ] {
            let num_channels = layout.num_channels();
            // Grey for `L`, which keeps a single channel. Opaque alpha, which video cannot carry.
            let pixel: Vec<u8> = match num_channels {
                1 => vec![7],
                3 => vec![1, 2, 3],
                _ => vec![1, 2, 3, 255],
            };
            let pixels = pixel.repeat(6);
            assert_eq!(
                layout.pixels_from_rgba(&layout.rgba_from_pixels(&pixels)),
                pixels,
                "{layout:?}"
            );
        }
    }

    #[test]
    fn colors_survive_i420() {
        let (width, height) = (16, 8);
        let rgba = [200, 30, 60, 255].repeat((width * height) as usize);
        let round_tripped = i420_to_rgba(&rgba_to_i420(&rgba, width, height));
        for (channel, round_tripped) in izip!(&rgba, &round_tripped) {
            assert!(
                channel.abs_diff(*round_tripped) <= 4,
                "{channel} became {round_tripped}"
            );
        }
    }
}
