//! The names and messages that the robot side and the operator side share in the room.

/// Topic of the byte streams that carry guaranteed Rerun log messages.
pub const LOG_TOPIC: &str = "rerun.log";

/// Name of the data track that carries latest-wins Rerun log messages, as [`LatestFrame`]s.
pub const LATEST_TRACK: &str = "rerun.latest";

/// One frame of [`LATEST_TRACK`]: the latest-wins messages that were logged together.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct LatestFrame {
    /// Encoded log messages.
    #[prost(bytes = "bytes", repeated, tag = "1")]
    pub log_msgs: Vec<bytes::Bytes>,
}
