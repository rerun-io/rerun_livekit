use livekit::DisconnectReason;
use livekit::prelude::{PublishError, RoomError, RpcError};
use livekit_api::access_token::AccessTokenError;

/// What can go wrong when joining the room or driving the robot.
#[derive(thiserror::Error, Debug)]
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
    InvalidLease { lease: String },

    /// The robot gave the lease to another driver, for example after it restarted.
    #[error("Lost the lease")]
    LeaseLost,

    #[error("The robot left the room")]
    RobotLeft,
}

pub type Result<T> = std::result::Result<T, Error>;
