//! Joins the LiveKit room of a `rerun-livekit` bridge, and sends commands to the robot
//! through it.
//!
//! The bridge itself, which carries a Rerun stream from a robot to operators, is the
//! `rerun-livekit` binary of this crate. This library is for the code at either end of
//! [`control`]: an input device that drives the robot, and the robot that follows it.

pub mod control;
mod error;
mod room;

pub use self::error::{Error, Result};
pub use self::room::{Role, RoomArgs};
