//! Joining the LiveKit room.

use livekit::prelude::*;
use livekit_api::access_token::{AccessToken, VideoGrants};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{Error, Result};

/// Participant attribute holding a [`Role`].
const ROLE_ATTRIBUTE: &str = "rerun.role";

/// What a participant does in the room.
///
/// It is set in the access token, so every other participant sees it as soon as this one joins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Publishes a Rerun stream.
    Robot,

    /// Receives a Rerun stream and feeds a viewer.
    Operator,

    /// Receives commands for the robot.
    RobotControl,

    /// Sends commands to the robot.
    Driver,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Self::Robot => "robot",
            Self::Operator => "operator",
            Self::RobotControl => "robot-control",
            Self::Driver => "driver",
        }
    }

    /// The role `participant` announced, if it is one of ours.
    pub fn of(participant: &RemoteParticipant) -> Option<Self> {
        let attributes = participant.attributes();
        [
            Self::Robot,
            Self::Operator,
            Self::RobotControl,
            Self::Driver,
        ]
        .into_iter()
        .find(|role| attributes.get(ROLE_ATTRIBUTE).map(String::as_str) == Some(role.as_str()))
    }
}

/// Where the room is and how to get into it.
#[derive(Clone, Debug, clap::Args)]
pub struct RoomArgs {
    /// LiveKit server URL.
    #[arg(long, env = "LIVEKIT_URL", default_value = "ws://127.0.0.1:7880")]
    url: String,

    /// API key. The default is the key of `livekit-server --dev`.
    #[arg(long, env = "LIVEKIT_API_KEY", default_value = "devkey")]
    api_key: String,

    /// API secret. The default is the secret of `livekit-server --dev`.
    #[arg(long, env = "LIVEKIT_API_SECRET", default_value = "secret")]
    api_secret: String,

    /// Room to join. Robot and operators must use the same one.
    #[arg(long, default_value = "rerun")]
    room: String,

    /// A name for this machine or person, to tell two participants with the same role apart.
    /// The identity in the room is the name, a dash, and the role, for example `alice-driver`.
    #[arg(long)]
    name: Option<String>,
}

impl RoomArgs {
    /// Joins the room as `role`, with a locally minted token.
    ///
    /// The identity in the room is the role, or `<name>-<role>` with `--name`. One process can
    /// join once per role, and two processes with the same role need different names.
    ///
    /// # Errors
    /// If the token cannot be made or the room cannot be reached.
    pub async fn connect(&self, role: Role) -> Result<(Room, UnboundedReceiver<RoomEvent>)> {
        let Self {
            url,
            api_key,
            api_secret,
            room,
            name,
        } = self;
        let identity = match name {
            Some(name) => format!("{name}-{}", role.as_str()),
            None => role.as_str().to_owned(),
        };

        let token = AccessToken::with_api_key(api_key, api_secret)
            .with_identity(&identity)
            .with_attributes([(ROLE_ATTRIBUTE, role.as_str())])
            .with_grants(VideoGrants {
                room_join: true,
                room: room.clone(),
                // The robot announces who holds the control lease in its attributes.
                can_update_own_metadata: Some(role == Role::RobotControl),
                ..Default::default()
            })
            .to_jwt()
            .map_err(|source| Error::Token { source })?;

        let mut options = RoomOptions::default();
        // Only the operator shows the robot's video. Nobody else should pay to receive it.
        options.auto_subscribe = role == Role::Operator;
        // Pause the simulcast layers of the robot's video that no operator is receiving.
        options.dynacast = role == Role::Robot;
        Room::connect(url, &token, options)
            .await
            .map_err(|source| Error::Connect {
                room: room.clone(),
                identity,
                url: url.clone(),
                source,
            })
    }
}
