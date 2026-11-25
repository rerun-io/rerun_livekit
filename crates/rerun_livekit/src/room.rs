//! Joining the LiveKit room.

use livekit::prelude::*;
use livekit_token::{AccessToken, VideoGrants};
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

    /// Whether a room has at most one participant with this role: the robot's roles.
    ///
    /// Without `--name`, their identity is the bare role, so that a robot that restarts replaces
    /// the participant it left behind, and a second robot in the room pushes the first one out
    /// instead of mixing in. That is why a robot should not be given a name.
    fn is_one_per_room(self) -> bool {
        match self {
            Self::Robot | Self::RobotControl => true,
            Self::Operator | Self::Driver => false,
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
///
/// Clones join with the same identity, so use one per participant.
#[derive(Clone, Debug, clap::Args)]
pub struct RoomArgs {
    /// LiveKit server URL.
    #[arg(long, env = "LIVEKIT_URL", default_value = "ws://127.0.0.1:7880")]
    url: String,

    /// API key. The default is the key of `livekit-server --dev`.
    #[arg(long, env = "LIVEKIT_API_KEY", default_value = "devkey")]
    api_key: String,

    /// API secret. The default is the secret of `livekit-server --dev`.
    #[arg(
        long,
        env = "LIVEKIT_API_SECRET",
        hide_env_values = true,
        default_value = "secret"
    )]
    api_secret: String,

    /// Room to join. One robot per room: the robot, its operators and its driver must use
    /// the same one. A second robot without `--name` pushes the first one out.
    #[arg(long, default_value = "rerun")]
    room: String,

    /// A name for this machine or person. The identity in the room is the name, a dash, and
    /// the role, for example `alice-driver`. Without it, operators and drivers get a random
    /// name, and the robot joins as `robot`.
    #[arg(long)]
    name: Option<String>,

    /// The name of an operator or a driver without `--name`. It stays the same for the life
    /// of this value, so that reconnecting replaces the participant that was left behind.
    #[arg(skip = random_name())]
    random_name: String,
}

fn random_name() -> String {
    format!("{:08x}", rand::random::<u32>())
}

impl RoomArgs {
    /// A room on a LiveKit server, for code that does not take these from the command line.
    pub fn new(
        url: impl Into<String>,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        room: impl Into<String>,
    ) -> Self {
        Self {
            url: url.into(),
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            room: room.into(),
            name: None,
            random_name: random_name(),
        }
    }

    /// Joins as `<name>-<role>`, in place of a random name for operators and drivers, and of the
    /// bare role for the robot's roles.
    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Joins the room as `role`, with a locally minted token.
    ///
    /// The identity in the room is `<name>-<role>`. Without `--name`, operators and drivers use
    /// a random name, so that any number of them can join, and the robot's roles use the bare
    /// role, so that the room has one robot.
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
            random_name,
        } = self;
        let name = match name {
            Some(name) => Some(name),
            None if role.is_one_per_room() => None,
            None => Some(random_name),
        };
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
                // The robot announces the current control lease in its attributes.
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
