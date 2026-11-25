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
//! - A command that arrives more than the watchdog timeout later than the fastest one of its
//!   lease counts as lost, so a link that starts holding commands back stops the robot
//!   instead of steering it late. The robot then drops the lease, which tells the driver.

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
/// LiveKit reserves 1001 to 1999.
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

    /// The smallest [`Received::delay_ms`] of the accepted commands. Comparing a delay with it
    /// cancels out the offset between the two clocks.
    min_delay_ms: Option<i64>,

    /// Whether the holder's last command was refused for arriving late. If the robot stops
    /// while it is, the lease is dropped, so that its driver learns of it.
    is_late: bool,

    /// When the holder last proved it is there: by taking the lease or by a valid command.
    last_heard: Instant,
}

impl Lease {
    /// A lease granted to `holder` just now.
    fn new(id: u64, holder: ParticipantIdentity) -> Self {
        Self {
            id,
            holder,
            last_seq: None,
            min_delay_ms: None,
            is_late: false,
            last_heard: Instant::now(),
        }
    }
}

/// What the watchdog did when no command arrived in time.
enum Stopped {
    /// The robot was already stopped.
    Nothing,

    /// The holder's commands stopped arriving. It keeps the lease, so that it can resume.
    Quiet,

    /// The holder's commands arrived late, so it lost the lease.
    Late(Lease),
}

/// A command as it reached the robot.
struct Received {
    sender: ParticipantIdentity,
    command: CommandMsg,

    /// When the command reached the robot, minus when the driver sent it, in milliseconds by
    /// each one's clock: the network delay plus the offset between the clocks.
    delay_ms: i64,
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
///
/// A command that arrives more than `watchdog_timeout` later than the fastest one of its lease
/// counts as lost. When the holder's commands keep arriving late, the robot stops and drops
/// the lease, and the driver gets [`Error::LeaseLost`]. That needs no synchronized clocks, and
/// it also happens if either clock jumps by more than `watchdog_timeout` during a lease.
///
/// The value stays `None` for good once another robot joins the room as this one, which has
/// room for one: the receiver then sees no more changes.
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

    let (command_tx, mut command_rx) = mpsc::channel::<Received>(4);
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

            Some(received) = command_rx.recv() => {
                // Followed while the lease is locked, so that a release cannot come in between
                // accepting the command and following it.
                lease_tx.send_if_modified(|leases| {
                    if let Some(payload) = accept(&mut leases.current, &received, watchdog_timeout) {
                        current_tx.send_replace(Some(payload));
                    }
                    false
                });
            }

            () = async { tokio::time::sleep_until(stop_at.unwrap_or_else(Instant::now)).await }, if stop_at.is_some() => {
                let mut stopped = Stopped::Nothing;
                lease_tx.send_if_modified(|leases| {
                    stopped = stop(leases, current_tx);
                    matches!(stopped, Stopped::Late(_))
                });
                match stopped {
                    Stopped::Nothing => {}
                    Stopped::Quiet => log::warn!("No command for {watchdog_timeout:?}, stopping"),
                    Stopped::Late(lease) => log::warn!(
                        "The commands of {} arrived more than {watchdog_timeout:?} late, \
                         stopping and dropping its lease",
                        lease.holder
                    ),
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

/// Stops the robot because no command arrived in time, and drops the lease if its holder's
/// commands were arriving late.
///
/// Call it with the lease locked, as everything that changes the command the robot follows is.
fn stop(leases: &mut Leases, current_tx: &watch::Sender<Option<Bytes>>) -> Stopped {
    let was_moving = current_tx.send_replace(None).is_some();
    match leases.current.take_if(|lease| lease.is_late) {
        Some(lease) => Stopped::Late(lease),
        None if was_moving => Stopped::Quiet,
        None => Stopped::Nothing,
    }
}

/// Returns the payload of a command if it comes from the lease holder, is newer than the last
/// one, and arrived no more than `max_lateness` later than the fastest one of the lease.
fn accept(lease: &mut Option<Lease>, received: &Received, max_lateness: Duration) -> Option<Bytes> {
    let lease = lease.as_mut()?;
    let Received {
        sender,
        command,
        delay_ms,
    } = received;
    let is_holders_next = lease.holder == *sender
        && lease.id == command.lease
        && lease.last_seq.is_none_or(|last_seq| last_seq < command.seq);
    if !is_holders_next {
        return None;
    }
    let max_lateness_ms = i64::try_from(max_lateness.as_millis()).unwrap_or(i64::MAX);
    lease.is_late = lease
        .min_delay_ms
        .is_some_and(|min_delay_ms| max_lateness_ms < delay_ms.saturating_sub(min_delay_ms));
    if lease.is_late {
        return None;
    }
    lease.last_seq = Some(command.seq);
    lease.min_delay_ms = Some(
        lease
            .min_delay_ms
            .map_or(*delay_ms, |min| min.min(*delay_ms)),
    );
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
        let (lease_tx, current_tx) = (lease_tx.clone(), current_tx.clone());
        move |invocation| {
            let (lease_tx, current_tx) = (lease_tx.clone(), current_tx.clone());
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
                    leases.current = Some(Lease::new(id, caller.clone()));
                    // A new lease starts stopped, so that nothing of the previous one is
                    // followed under it.
                    current_tx.send_replace(None);
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
async fn forward_commands(track: RemoteDataTrack, command_tx: mpsc::Sender<Received>) {
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
        // How late a command is cannot be told without the time it was sent.
        let Some(delay_ms) = frame.user_timestamp().and_then(delay_since) else {
            log::warn!("Ignoring the commands of {sender}: they carry no time they were sent");
            return;
        };
        let received = Received {
            sender: sender.clone(),
            command,
            delay_ms,
        };
        if command_tx.send(received).await.is_err() {
            return;
        }
    }
}

/// Milliseconds from `sent_at_ms`, by the driver's clock, until now, by this one's.
fn delay_since(sent_at_ms: u64) -> Option<i64> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    Some(i64::try_from(now.as_millis()).ok()? - i64::try_from(sent_at_ms).ok()?)
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
        // The time it is sent tells the robot whether it arrived late.
        let frame = DataTrackFrame::new(command.encode_to_vec()).with_user_timestamp_now();
        // A full queue means the track is being published again after a reconnect, or LiveKit's
        // pipeline fell behind, and the next command replaces this one either way. An
        // unpublished track comes after the robot left or the room disconnected, which the next
        // call reports.
        self.track.try_push(frame).ok();
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

    const MAX_LATENESS: Duration = Duration::from_millis(300);

    fn received(sender: &str, lease: u64, seq: u64, delay_ms: i64) -> Received {
        Received {
            sender: ParticipantIdentity(sender.to_owned()),
            command: CommandMsg {
                lease,
                seq,
                payload: Bytes::from_static(b"forward"),
            },
            delay_ms,
        }
    }

    fn lease_of_alice() -> Lease {
        Lease::new(1, ParticipantIdentity("alice".to_owned()))
    }

    #[test]
    fn only_newer_commands_from_the_holder_are_accepted() {
        let accept = |lease: &mut _, received| accept(lease, &received, MAX_LATENESS);
        let payload = Some(Bytes::from_static(b"forward"));

        assert_eq!(
            accept(&mut None, received("alice", 1, 1, 0)),
            None,
            "nobody holds the lease"
        );

        let mut lease = Some(lease_of_alice());
        assert_eq!(accept(&mut lease, received("alice", 1, 5, 0)), payload);
        assert_eq!(
            accept(&mut lease, received("alice", 1, 5, 0)),
            None,
            "duplicate"
        );
        assert_eq!(
            accept(&mut lease, received("alice", 1, 4, 0)),
            None,
            "out of order"
        );
        assert_eq!(
            accept(&mut lease, received("bob", 1, 6, 0)),
            None,
            "not the holder"
        );
        assert_eq!(
            accept(&mut lease, received("alice", 2, 6, 0)),
            None,
            "an old lease"
        );
        assert_eq!(accept(&mut lease, received("alice", 1, 6, 0)), payload);
    }

    #[test]
    fn a_command_later_than_the_fastest_by_more_than_the_limit_is_lost() {
        let accept = |lease: &mut _, received| accept(lease, &received, MAX_LATENESS).is_some();
        let mut lease = Some(lease_of_alice());

        // The driver's clock is five seconds behind the robot's.
        assert!(accept(&mut lease, received("alice", 1, 1, 5_050)));
        assert!(
            accept(&mut lease, received("alice", 1, 2, 5_300)),
            "within the limit"
        );
        assert!(
            !accept(&mut lease, received("alice", 1, 3, 5_400)),
            "beyond the limit"
        );
        let is_late = |lease: &Option<Lease>| lease.as_ref().is_some_and(|lease| lease.is_late);
        assert!(is_late(&lease));
        accept(&mut lease, received("bob", 1, 4, 5_050));
        assert!(
            is_late(&lease),
            "a command of another driver changes nothing"
        );

        assert!(
            accept(&mut lease, received("alice", 1, 4, 5_020)),
            "faster than any before"
        );
        assert!(!is_late(&lease));
        assert!(
            !accept(&mut lease, received("alice", 1, 5, 5_340)),
            "beyond the limit of the faster one"
        );
    }

    #[test]
    fn the_watchdog_drops_only_a_late_lease() {
        let mut leases = Leases::new();
        leases.current = Some(lease_of_alice());
        let (current_tx, _current_rx) = watch::channel(None);
        let follow = |leases: &mut Leases, received: Received| {
            if let Some(payload) = accept(&mut leases.current, &received, MAX_LATENESS) {
                current_tx.send_replace(Some(payload));
            }
        };

        follow(&mut leases, received("alice", 1, 1, 0));
        assert!(matches!(stop(&mut leases, &current_tx), Stopped::Quiet));
        assert!(
            matches!(stop(&mut leases, &current_tx), Stopped::Nothing),
            "already stopped"
        );
        assert!(leases.current.is_some(), "a quiet holder may resume");

        follow(&mut leases, received("alice", 1, 2, 0));
        follow(&mut leases, received("alice", 1, 3, 1_000));
        assert!(matches!(stop(&mut leases, &current_tx), Stopped::Late(_)));
        assert!(
            is_lease_lost(&leases.announced_id().to_string(), 1),
            "the late holder learns that it lost the lease"
        );
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
        leases.current = Some(Lease::new(next_id, alice.clone()));
        leases.next_id += 1;
        assert_eq!(leases.announced_id(), next_id, "the current lease");

        assert!(leases.drop_held_by(&alice));
        assert!(
            is_lease_lost(&leases.announced_id().to_string(), next_id),
            "dropping the lease tells its driver"
        );
    }
}
