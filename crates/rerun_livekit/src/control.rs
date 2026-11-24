//! Commands from an operator back to the robot.
//!
//! A command is whatever bytes the two ends agree on: a twist, joint targets, a pose from a
//! VR controller, a gamepad state. This module only gets the newest one to the robot safely.
//!
//! It follows LiveKit's teleoperation pattern and has nothing to do with Rerun:
//! - The driver sends the whole command at a fixed rate on a lossy data track, so a lost or
//!   late command is replaced by the next one instead of being retried.
//! - One driver at a time holds a lease, which it gets from the robot with an RPC.
//! - The robot stops when valid commands stop arriving. From then on another driver may take
//!   the lease, so a driver that crashed does not lock everyone else out.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt as _;
use livekit::prelude::*;
use prost::Message as _;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::error::with_causes;
use crate::room::{Role, RoomArgs};
use crate::{Error, Result};

/// Name of the data track a driver publishes its [`CommandMsg`]s on.
const COMMAND_TRACK: &str = "teleop.cmd";

/// RPC on the robot that gives the caller the lease. Returns the lease id.
const ACQUIRE_METHOD: &str = "teleop.acquire";

/// RPC on the robot that gives the lease back.
const RELEASE_METHOD: &str = "teleop.release";

/// Attribute of the robot's participant that holds the id of the current lease, in decimal.
/// Absent while no driver holds the lease.
///
/// Every grant has a new id, so a driver always sees its own grant announced, even when it
/// re-acquires under the same identity.
const LEASE_ATTRIBUTE: &str = "teleop.lease";

/// RPC error code for "another driver holds the lease", with the holder as its data.
/// LiveKit reserves 1000 and up.
const LEASE_HELD_ERROR: u32 = 1;

/// How long the robot waits before it starts a new session after losing the room.
const RETRY_DELAY: Duration = Duration::from_secs(2);

/// A watchdog timeout for [`receive_commands`] that suits a driver sending at 30 Hz: about ten
/// lost commands in a row stop the robot.
pub const DEFAULT_WATCHDOG_TIMEOUT: Duration = Duration::from_millis(300);

/// What a driver sends, many times a second.
#[derive(Clone, PartialEq, prost::Message)]
struct CommandMsg {
    /// The id the robot returned from [`ACQUIRE_METHOD`].
    #[prost(uint64, tag = "1")]
    lease: u64,

    /// Increases with every command, so the robot can ignore one that arrives out of order.
    #[prost(uint64, tag = "2")]
    seq: u64,

    /// The command itself, in whatever encoding the driver and the robot agree on.
    #[prost(bytes = "bytes", tag = "3")]
    payload: Bytes,
}

// ----------------------------------------------------------------------------
// Robot side

/// The driver that may command the robot.
struct Lease {
    id: u64,
    holder: ParticipantIdentity,

    /// Sequence number of the last command that was accepted.
    last_seq: Option<u64>,

    /// When the holder last proved it is there: by taking the lease or by a valid command.
    last_heard: Instant,
}

/// Joins the room and returns the command the robot should follow right now.
///
/// The value is `None`, and the robot must stop, whenever no driver holds the lease, or the
/// holder's commands stopped arriving for `watchdog_timeout`, or the room is unreachable.
pub fn receive_commands(
    room: RoomArgs,
    watchdog_timeout: Duration,
) -> watch::Receiver<Option<Bytes>> {
    let (current_tx, current_rx) = watch::channel(None);
    tokio::spawn(async move {
        loop {
            let Err(err) = receive_session(&room, watchdog_timeout, &current_tx).await;
            current_tx.send_replace(None);
            log::warn!(
                "Command session ended, starting a new one in {RETRY_DELAY:?}: {}",
                with_causes(&err)
            );
            tokio::time::sleep(RETRY_DELAY).await;
        }
    });
    current_rx
}

async fn receive_session(
    room_args: &RoomArgs,
    watchdog_timeout: Duration,
    current_tx: &watch::Sender<Option<Bytes>>,
) -> Result<std::convert::Infallible> {
    let (room, mut events) = room_args.connect(Role::RobotControl).await?;
    let local = room.local_participant();
    // The only copy of the lease. Granting or dropping it notifies `lease_rx`, so that the
    // room hears of it. Accepting a command does not.
    let lease_tx = watch::Sender::new(None::<Lease>);
    let mut lease_rx = lease_tx.subscribe();
    register_lease_methods(&local, &lease_tx, watchdog_timeout, current_tx);
    log::info!("Waiting for a driver");

    let (command_tx, mut command_rx) = mpsc::channel::<(ParticipantIdentity, CommandMsg)>(4);
    let result = loop {
        // When to stop unless another command arrives. `None` while already stopped.
        let stop_at = lease_tx
            .borrow()
            .as_ref()
            .filter(|_| current_tx.borrow().is_some())
            .map(|lease| lease.last_heard + watchdog_timeout);

        tokio::select! {
            event = events.recv() => match event {
                Some(RoomEvent::DataTrackPublished(track)) if track.info().name() == COMMAND_TRACK => {
                    tokio::spawn(forward_commands(track, command_tx.clone()));
                }
                Some(RoomEvent::Disconnected { reason }) => break Err(Error::Disconnected { reason }),
                Some(_) => {}
                None => break Err(Error::RoomClosed),
            },

            Some((sender, command)) = command_rx.recv() => {
                let mut payload = None;
                lease_tx.send_if_modified(|lease| {
                    payload = accept(lease, &sender, &command);
                    false
                });
                if let Some(payload) = payload {
                    current_tx.send_replace(Some(payload));
                }
            }

            // The only place that announces the lease, always as it is now, so that the last
            // announcement is the truth.
            Ok(()) = lease_rx.changed() => {
                let lease_id = lease_rx.borrow_and_update().as_ref().map(|lease| lease.id);
                announce_lease(&local, lease_id).await;
            }

            () = async { tokio::time::sleep_until(stop_at.unwrap_or_else(Instant::now)).await }, if stop_at.is_some() => {
                if current_tx.send_replace(None).is_some() {
                    log::warn!("No command for {watchdog_timeout:?}, stopping");
                }
            }
        }
    };
    room.close().await.ok();
    result
}

/// Returns the payload of `command` if it comes from the lease holder and is newer than the last one.
fn accept(
    lease: &mut Option<Lease>,
    sender: &ParticipantIdentity,
    command: &CommandMsg,
) -> Option<Bytes> {
    let lease = lease.as_mut()?;
    let is_valid = lease.holder == *sender
        && lease.id == command.lease
        && lease.last_seq.is_none_or(|last_seq| last_seq < command.seq);
    if !is_valid {
        return None;
    }
    lease.last_seq = Some(command.seq);
    lease.last_heard = Instant::now();
    Some(command.payload.clone())
}

/// Tells everyone in the room which lease is current, if any.
async fn announce_lease(local: &LocalParticipant, lease_id: Option<u64>) {
    // An empty value removes the attribute.
    let value = lease_id.map(|id| id.to_string()).unwrap_or_default();
    let attributes = HashMap::from([(LEASE_ATTRIBUTE.to_owned(), value)]);
    if let Err(err) = local.set_attributes(attributes).await {
        log::warn!("Failed to announce the lease: {err}");
    }
}

fn register_lease_methods(
    local: &LocalParticipant,
    lease_tx: &watch::Sender<Option<Lease>>,
    watchdog_timeout: Duration,
    current_tx: &watch::Sender<Option<Bytes>>,
) {
    local.register_rpc_method(ACQUIRE_METHOD.to_owned(), {
        let lease_tx = lease_tx.clone();
        move |invocation| {
            let lease_tx = lease_tx.clone();
            Box::pin(async move {
                let caller = invocation.caller_identity;
                // Decided and granted in one step, so that two drivers cannot both get it.
                let mut granted = Err(String::new());
                lease_tx.send_if_modified(|lease| {
                    let driving = lease.as_ref().filter(|held| {
                        held.holder != caller && held.last_heard.elapsed() < watchdog_timeout
                    });
                    if let Some(held) = driving {
                        granted = Err(held.holder.to_string());
                        return false;
                    }
                    let id = rand::random();
                    *lease = Some(Lease {
                        id,
                        holder: caller.clone(),
                        last_seq: None,
                        last_heard: Instant::now(),
                    });
                    granted = Ok(id);
                    true
                });
                match granted {
                    Ok(id) => {
                        log::info!("{caller} got the lease");
                        Ok(id.to_string())
                    }
                    Err(holder) => {
                        let message = format!("{holder} is driving");
                        Err(RpcError::new(LEASE_HELD_ERROR, message, Some(holder)))
                    }
                }
            })
        }
    });

    local.register_rpc_method(RELEASE_METHOD.to_owned(), {
        let (lease_tx, current_tx) = (lease_tx.clone(), current_tx.clone());
        move |invocation| {
            let (lease_tx, current_tx) = (lease_tx.clone(), current_tx.clone());
            Box::pin(async move {
                let caller = invocation.caller_identity;
                let was_holder = lease_tx.send_if_modified(|lease| {
                    let was_holder = lease.as_ref().is_some_and(|held| held.holder == caller);
                    if was_holder {
                        *lease = None;
                    }
                    was_holder
                });
                if was_holder {
                    current_tx.send_replace(None);
                    log::info!("{caller} released the lease");
                }
                Ok(String::new())
            })
        }
    });
}

/// Forwards the commands on `track` until the track or the receiver goes away.
async fn forward_commands(
    track: RemoteDataTrack,
    command_tx: mpsc::Sender<(ParticipantIdentity, CommandMsg)>,
) {
    let sender = ParticipantIdentity(track.publisher_identity().to_owned());
    // Only the newest command matters, so keep LiveKit from queueing old ones.
    let options = DataTrackSubscribeOptions::new().with_buffer_size(1);
    let mut frames = match track.subscribe_with_options(options).await {
        Ok(frames) => frames,
        Err(err) => {
            log::warn!("Failed to subscribe to the commands of {sender}: {err}");
            return;
        }
    };
    while let Some(frame) = frames.next().await {
        let Ok(command) = CommandMsg::decode(frame.payload()) else {
            continue;
        };
        if command_tx.send((sender.clone(), command)).await.is_err() {
            return;
        }
    }
}

// ----------------------------------------------------------------------------
// Driver side

/// A driver's hold on the robot. Send it the commands the robot should follow.
pub struct Driver {
    room: Room,
    events: mpsc::UnboundedReceiver<RoomEvent>,
    robot: ParticipantIdentity,
    track: LocalDataTrack,
    lease: u64,
    seq: u64,
    lease_watch: LeaseWatch,
}

impl Driver {
    /// Joins the room and takes the lease from its robot. A room has one robot.
    ///
    /// # Errors
    /// [`Error::LeaseHeld`] if another driver holds the lease, or another [`Error`] if the
    /// room cannot be reached.
    pub async fn acquire(room_args: &RoomArgs) -> Result<Self> {
        let (room, mut events) = room_args.connect(Role::Driver).await?;

        let is_robot =
            |participant: &RemoteParticipant| Role::of(participant) == Some(Role::RobotControl);
        let robot = if let Some(robot) = room.remote_participants().into_values().find(is_robot) {
            robot
        } else {
            log::info!("Waiting for the robot to join the room");
            loop {
                if let RoomEvent::ParticipantActive(participant) =
                    events.recv().await.ok_or(Error::RoomClosed)?
                    && is_robot(&participant)
                {
                    break participant;
                }
            }
        }
        .identity();

        let lease = room
            .local_participant()
            .perform_rpc(PerformRpcData::new(robot.clone(), ACQUIRE_METHOD))
            .await
            .map_err(|source| match source.data.clone() {
                // LiveKit turns a missing `data` into an empty string.
                Some(holder) if source.code == LEASE_HELD_ERROR && !holder.is_empty() => {
                    Error::LeaseHeld { holder }
                }
                _ => Error::LeaseRefused { source },
            })?;
        let lease = lease
            .parse()
            .map_err(|source| Error::InvalidLease { lease, source })?;
        let track = room
            .local_participant()
            .publish_data_track(COMMAND_TRACK)
            .await
            .map_err(|source| Error::PublishCommandTrack { source })?;

        let lease_watch = LeaseWatch::new(lease);
        Ok(Self {
            room,
            events,
            robot,
            track,
            lease,
            seq: 0,
            lease_watch,
        })
    }

    /// Sends a command. `payload` is the whole command, not a change to the previous one.
    ///
    /// Call this at a fixed rate, several times within the robot's watchdog timeout: the robot
    /// stops when commands stop. Keep the payload under about a kilobyte, so that it fits in
    /// one packet and a single lost packet cannot cost more than one command.
    ///
    /// # Errors
    /// Once the lease is gone: [`Error::LeaseLost`], [`Error::RobotLeft`] or
    /// [`Error::Disconnected`].
    pub fn send(&mut self, payload: impl Into<Bytes>) -> Result<()> {
        while let Ok(event) = self.events.try_recv() {
            match event {
                RoomEvent::ParticipantAttributesChanged {
                    participant,
                    changed_attributes,
                } if participant.identity() == self.robot => {
                    if let Some(lease) = changed_attributes.get(LEASE_ATTRIBUTE) {
                        self.lease_watch.on_announced(lease)?;
                    }
                }
                RoomEvent::ParticipantDisconnected(participant)
                    if participant.identity() == self.robot =>
                {
                    return Err(Error::RobotLeft);
                }
                RoomEvent::Disconnected { reason } => return Err(Error::Disconnected { reason }),
                _ => {}
            }
        }

        self.seq += 1;
        let command = CommandMsg {
            lease: self.lease,
            seq: self.seq,
            payload: payload.into(),
        };
        // A full queue means the previous command is still going out. The next one replaces this.
        self.track
            .try_push(DataTrackFrame::new(command.encode_to_vec()))
            .ok();
        Ok(())
    }

    /// Gives the lease back and leaves the room.
    pub async fn release(self) {
        let request = PerformRpcData::new(self.robot, RELEASE_METHOD);
        if let Err(err) = self.room.local_participant().perform_rpc(request).await {
            log::warn!("Failed to release the lease: {}", err.message);
        }
        self.room.close().await.ok();
    }
}

/// Whether a driver still holds its lease, going by the lease the robot announces.
///
/// Announcements made before the robot announced this driver's lease are stale: they come from
/// before the acquire, such as the previous driver's release. Only a later announcement of
/// another lease, or of none, means that this one is gone.
struct LeaseWatch {
    lease: String,
    has_been_announced: bool,
}

impl LeaseWatch {
    fn new(lease: u64) -> Self {
        Self {
            lease: lease.to_string(),
            has_been_announced: false,
        }
    }

    /// Takes in a lease the robot announced, as the value of [`LEASE_ATTRIBUTE`]. Empty means none.
    fn on_announced(&mut self, lease: &str) -> Result<()> {
        if lease == self.lease {
            self.has_been_announced = true;
            Ok(())
        } else if self.has_been_announced {
            Err(Error::LeaseLost)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(lease: u64, seq: u64) -> CommandMsg {
        CommandMsg {
            lease,
            seq,
            payload: Bytes::from_static(b"forward"),
        }
    }

    #[test]
    fn only_newer_commands_from_the_holder_are_accepted() {
        let alice = ParticipantIdentity("alice".to_owned());
        let bob = ParticipantIdentity("bob".to_owned());

        assert_eq!(
            accept(&mut None, &alice, &command(1, 1)),
            None,
            "nobody holds the lease"
        );

        let mut lease = Some(Lease {
            id: 1,
            holder: alice.clone(),
            last_seq: None,
            last_heard: Instant::now(),
        });
        let payload = Some(Bytes::from_static(b"forward"));
        assert_eq!(accept(&mut lease, &alice, &command(1, 5)), payload);
        assert_eq!(
            accept(&mut lease, &alice, &command(1, 5)),
            None,
            "duplicate"
        );
        assert_eq!(
            accept(&mut lease, &alice, &command(1, 4)),
            None,
            "out of order"
        );
        assert_eq!(
            accept(&mut lease, &bob, &command(1, 6)),
            None,
            "not the holder"
        );
        assert_eq!(
            accept(&mut lease, &alice, &command(2, 6)),
            None,
            "an old lease"
        );
        assert_eq!(accept(&mut lease, &alice, &command(1, 6)), payload);
    }

    #[test]
    fn only_a_later_announcement_takes_the_lease() {
        let holds_after = |announcements: &[&str]| {
            let mut watch = LeaseWatch::new(7);
            announcements
                .iter()
                .try_for_each(|lease| watch.on_announced(lease))
                .is_ok()
        };

        assert!(holds_after(&["", "7"]), "the previous driver's release");
        assert!(holds_after(&["3", "7"]), "the previous lease");
        assert!(holds_after(&["7", "7"]), "announced again");
        assert!(!holds_after(&["7", "9"]), "another driver took over");
        assert!(!holds_after(&["7", ""]), "the robot dropped the lease");
    }
}
