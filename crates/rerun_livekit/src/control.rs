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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

/// Attribute of the robot's participant that holds, in decimal, the id of the current lease,
/// or while no driver holds one, the id the next lease will get.
///
/// Lease ids only grow, so a larger value than a driver's own lease means that its lease is
/// gone: another driver took over, or the robot dropped it.
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

/// The robot's lease, and the id of the next one it grants.
struct Leases {
    current: Option<Lease>,

    /// Starts at the session's start time in microseconds since the Unix epoch, and grows by
    /// one with every grant. Ids grow within a session, and across sessions and restarts as
    /// long as the clock does. A driver learns of a new session from the robot leaving anyway.
    next_id: u64,
}

impl Leases {
    fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Self {
            current: None,
            next_id: u64::try_from(now.as_micros()).unwrap_or(u64::MAX),
        }
    }

    /// The value of [`LEASE_ATTRIBUTE`].
    fn announced_id(&self) -> u64 {
        self.current.as_ref().map_or(self.next_id, |lease| lease.id)
    }

    /// Drops the lease if `holder` holds it. Returns whether it did.
    fn drop_held_by(&mut self, holder: &ParticipantIdentity) -> bool {
        let is_held = self
            .current
            .as_ref()
            .is_some_and(|lease| lease.holder == *holder);
        if is_held {
            self.current = None;
        }
        is_held
    }
}

/// Joins the room and returns the command the robot should follow right now.
///
/// The value is `None`, and the robot must stop, whenever no driver holds the lease, or the
/// holder's commands stopped arriving for `watchdog_timeout`, or the room is unreachable.
/// It stays `None` for good once another robot joins the room as this one, which has room for
/// one: the receiver then sees no more changes.
pub fn receive_commands(
    room: RoomArgs,
    watchdog_timeout: Duration,
) -> watch::Receiver<Option<Bytes>> {
    let (current_tx, current_rx) = watch::channel(None);
    tokio::spawn(async move {
        loop {
            let Err(err) = receive_session(&room, watchdog_timeout, &current_tx).await;
            current_tx.send_replace(None);
            if err.is_replaced() {
                log::error!("Another robot joined the room, so this one no longer takes commands");
                // Keeps `current_tx` alive, so that receivers wait instead of seeing it closed.
                std::future::pending::<()>().await;
            }
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
    let lease_tx = watch::Sender::new(Leases::new());
    let announcer = tokio::spawn(announce_leases(local.clone(), lease_tx.subscribe()));
    register_lease_methods(&local, &lease_tx, watchdog_timeout, current_tx);
    log::info!("Waiting for a driver");

    let (command_tx, mut command_rx) = mpsc::channel::<(ParticipantIdentity, CommandMsg)>(4);
    let result = loop {
        // When to stop unless another command arrives. `None` while already stopped.
        let stop_at = lease_tx
            .borrow()
            .current
            .as_ref()
            .filter(|_| current_tx.borrow().is_some())
            .map(|lease| lease.last_heard + watchdog_timeout);

        tokio::select! {
            event = events.recv() => match event {
                Some(RoomEvent::DataTrackPublished(track)) if track.info().name() == COMMAND_TRACK => {
                    tokio::spawn(forward_commands(track, command_tx.clone()));
                }
                Some(RoomEvent::ParticipantDisconnected(participant)) => {
                    let identity = participant.identity();
                    // A participant that rejoined under the same identity is back already, and
                    // the lease may be its own by now.
                    let has_left = !room.remote_participants().contains_key(&identity);
                    let had_lease = has_left
                        && lease_tx.send_if_modified(|leases| drop_lease(leases, &identity, current_tx));
                    if had_lease {
                        log::info!("{identity} left and lost the lease");
                    }
                }
                Some(RoomEvent::Disconnected { reason }) => break Err(Error::Disconnected { reason }),
                Some(_) => {}
                None => break Err(Error::RoomClosed),
            },

            Some((sender, command)) = command_rx.recv() => {
                // Followed while the lease is locked, so that a release cannot come in between
                // accepting the command and following it.
                lease_tx.send_if_modified(|leases| {
                    if let Some(payload) = accept(&mut leases.current, &sender, &command) {
                        current_tx.send_replace(Some(payload));
                    }
                    false
                });
            }

            () = async { tokio::time::sleep_until(stop_at.unwrap_or_else(Instant::now)).await }, if stop_at.is_some() => {
                if current_tx.send_replace(None).is_some() {
                    log::warn!("No command for {watchdog_timeout:?}, stopping");
                }
            }
        }
    };
    announcer.abort();
    room.close().await.ok();
    result
}

/// Drops the lease if `holder` holds it, and stops the robot. Returns whether it did.
///
/// Call it with the lease locked, as everything that changes the command the robot follows is.
fn drop_lease(
    leases: &mut Leases,
    holder: &ParticipantIdentity,
    current_tx: &watch::Sender<Option<Bytes>>,
) -> bool {
    let is_dropped = leases.drop_held_by(holder);
    if is_dropped {
        current_tx.send_replace(None);
    }
    is_dropped
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

/// Keeps [`LEASE_ATTRIBUTE`] up to date, from the start of the session on.
///
/// It is the only writer of the attribute, it always writes the lease as it is now, and it
/// retries until a write succeeds, so the attribute ends up true. It runs apart from the
/// session loop, because an announcement can take seconds and the watchdog must not wait for it.
async fn announce_leases(local: LocalParticipant, mut lease_rx: watch::Receiver<Leases>) {
    loop {
        let value = lease_rx.borrow_and_update().announced_id().to_string();
        let attributes = HashMap::from([(LEASE_ATTRIBUTE.to_owned(), value)]);
        match local.set_attributes(attributes).await {
            Ok(()) => {
                if lease_rx.changed().await.is_err() {
                    return;
                }
            }
            Err(err) => {
                log::warn!("Failed to announce the lease, trying again in {RETRY_DELAY:?}: {err}");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}

fn register_lease_methods(
    local: &LocalParticipant,
    lease_tx: &watch::Sender<Leases>,
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
                lease_tx.send_if_modified(|leases| {
                    let driving = leases.current.as_ref().filter(|held| {
                        held.holder != caller && held.last_heard.elapsed() < watchdog_timeout
                    });
                    if let Some(held) = driving {
                        granted = Err(held.holder.to_string());
                        return false;
                    }
                    let id = leases.next_id;
                    leases.next_id += 1;
                    leases.current = Some(Lease {
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
                let was_holder =
                    lease_tx.send_if_modified(|leases| drop_lease(leases, &caller, &current_tx));
                if was_holder {
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

        Ok(Self {
            room,
            events,
            robot,
            track,
            lease,
            seq: 0,
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
                    if changed_attributes
                        .get(LEASE_ATTRIBUTE)
                        .is_some_and(|announced| is_lease_lost(announced, self.lease))
                    {
                        return Err(Error::LeaseLost);
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
        // A full queue means the track is being published again after a reconnect, and the
        // next command replaces this one. An unpublished track comes after the robot left or
        // the room disconnected, which the next call reports.
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

/// Whether the robot announcing `announced` as its [`LEASE_ATTRIBUTE`] means that `lease` is
/// gone. Earlier values, from before this lease was granted, are smaller or equal.
fn is_lease_lost(announced: &str, lease: u64) -> bool {
    announced
        .parse::<u64>()
        .is_ok_and(|announced| lease < announced)
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
    fn only_a_larger_announcement_takes_this_lease() {
        assert!(
            !is_lease_lost("7", 7),
            "this lease, or no lease before it was granted"
        );
        assert!(!is_lease_lost("3", 7), "an earlier lease");
        assert!(is_lease_lost("8", 7), "the robot dropped this lease");
        assert!(is_lease_lost("9", 7), "another driver took over");
        assert!(
            !is_lease_lost("", 7),
            "a robot that has announced nothing yet"
        );
    }

    #[test]
    fn without_a_lease_the_next_id_is_announced() {
        let mut leases = Leases::new();
        let next_id = leases.next_id;
        assert_eq!(leases.announced_id(), next_id);

        let alice = ParticipantIdentity("alice".to_owned());
        leases.current = Some(Lease {
            id: next_id,
            holder: alice.clone(),
            last_seq: None,
            last_heard: Instant::now(),
        });
        leases.next_id += 1;
        assert_eq!(leases.announced_id(), next_id, "the current lease");

        assert!(leases.drop_held_by(&alice));
        assert!(
            is_lease_lost(&leases.announced_id().to_string(), next_id),
            "dropping the lease tells its driver"
        );
    }
}
