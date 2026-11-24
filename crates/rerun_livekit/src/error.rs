use std::fmt::Write as _;

use livekit::DisconnectReason;
use livekit::prelude::{PublishError, RoomError, RpcError};
use livekit_api::access_token::AccessTokenError;

/// What can go wrong when joining the room or driving the robot.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to create the LiveKit access token")]
    Token { source: AccessTokenError },

    #[error("Failed to join room {room:?} as {identity:?} at {url}")]
    Connect {
        room: String,
        identity: String,
        url: String,
        source: RoomError,
    },

    #[error("Failed to publish the command track")]
    PublishCommandTrack { source: PublishError },

    #[error("The room closed")]
    RoomClosed,

    #[error("Disconnected from the room: {reason:?}")]
    Disconnected { reason: DisconnectReason },

    /// Another driver holds the lease and is still sending commands.
    #[error("{holder} is driving")]
    LeaseHeld { holder: String },

    #[error("The robot refused the lease")]
    LeaseRefused { source: RpcError },

    #[error("The robot returned an invalid lease {lease:?}")]
    InvalidLease {
        lease: String,
        source: std::num::ParseIntError,
    },

    /// The robot gave the lease to another driver, for example after it restarted.
    #[error("Lost the lease")]
    LeaseLost,

    #[error("The robot left the room")]
    RobotLeft,
}

impl Error {
    /// Whether another participant joined the room with this identity, which pushed this one
    /// out. Rejoining would push that one out in turn, so do not retry.
    pub fn is_replaced(&self) -> bool {
        matches!(
            self,
            Self::Disconnected {
                reason: DisconnectReason::DuplicateIdentity
            }
        )
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// `err` followed by each of its causes, separated by `": "`.
///
/// [`Error`]'s `Display` is only the top line, as is usual in Rust, so that reporters like
/// `anyhow` can print the causes without repeating them. Logs of this crate use this instead.
pub fn with_causes(err: &dyn std::error::Error) -> String {
    let mut text = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        write!(text, ": {cause}").ok();
        source = cause.source();
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_duplicate_identity_replaces() {
        let disconnected = |reason| Error::Disconnected { reason };
        assert!(disconnected(DisconnectReason::DuplicateIdentity).is_replaced());
        assert!(!disconnected(DisconnectReason::ServerShutdown).is_replaced());
    }

    #[test]
    fn causes_follow_the_top_line() {
        let source = "x".parse::<u64>().unwrap_err();
        let err = Error::InvalidLease {
            lease: "x".to_owned(),
            source,
        };
        assert_eq!(
            with_causes(&err),
            "The robot returned an invalid lease \"x\": invalid digit found in string"
        );
    }
}
