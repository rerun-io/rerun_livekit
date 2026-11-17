//! Carries any Rerun stream over a LiveKit room, for teleoperation.
//!
//! [`robot`] reads the stream from a Rerun proxy next to the robot and publishes it to a room.
//! [`operator`] receives it from the room and writes it to a viewer next to the operator.
//! [`control`] sends commands the other way, and does not depend on the other two.

mod classify;
pub mod control;
mod latency;
pub mod operator;
mod proxy;
pub mod robot;
mod room;
mod rules;
mod video;

pub use self::room::{Role, RoomArgs};
pub use self::video::VideoCodecArg;
