# rerun-livekit

Carry any [Rerun](https://rerun.io) stream over a [LiveKit](https://livekit.com) room, so a remote operator can watch a robot in a normal Rerun viewer and drive it.

This is an example of using the two together. It needs no changes to Rerun, to LiveKit, or to the app that logs.

```
robot app ─connect_grpc─▶ rerun --serve-grpc ─▶ rerun-livekit robot ─┐
                                                                     │ LiveKit room
        Rerun viewer ◀──────────────── rerun-livekit operator ◀──────┘

your operator input ──any command bytes──▶ your robot code   (rerun_livekit::control)
```

- **Any Rerun app works.** The robot side reads from the gRPC proxy that every Rerun viewer and `rerun --serve-grpc` hosts, so the app can be written with any Rerun SDK and does not know the bridge exists.
- **The operator sees the same recording.** Entity paths, archetypes, components, timelines, row ids and the blueprint are the same on both sides. Only pixel values (video is lossy) and rates (what you choose to drop) differ.
- **Images travel as video.** `Image` and `EncodedImage` go over LiveKit video tracks, with hardware encoding and congestion control, and come back as `Image` and `EncodedImage`.
- **Late joiners get the whole scene.** An operator who joins later receives the blueprint and the static data first (the newest 256 MiB of it by default, see `--replay-limit-bytes`).

## Try it on one machine

Install the [LiveKit server](https://docs.livekit.io/transport/self-hosting/local/) and the [Rerun viewer](https://rerun.io/docs/getting-started/install-rerun), then run each of these in its own terminal:

```sh
livekit-server --dev                       # the room server, with its built-in dev key

rerun --serve-grpc --port 9876             # robot side: the proxy the app logs to
rerun --port 9877                          # operator side: the viewer

cargo run --release -- robot
cargo run --release -- operator --viewer rerun+http://127.0.0.1:9877/proxy

cargo run --release --example sim_robot    # a simulated robot that logs to port 9876
cargo run --release --example keyboard_teleop   # drive it: w/x a/d, space to stop, q to quit
```

The viewer shows the simulated robot, its camera, and the commands it receives. Start `rerun-livekit robot` before the app, or the first seconds of data are skipped as too old.

Any other app works in place of `sim_robot`: point it at `rerun+http://127.0.0.1:9876/proxy`, which is the default of `connect_grpc()`.

### Two machines

Give every command the same room: `--url`, `--api-key`, `--api-secret` and `--room`, or the `LIVEKIT_URL`, `LIVEKIT_API_KEY` and `LIVEKIT_API_SECRET` environment variables. Use [LiveKit Cloud](https://cloud.livekit.io) or [host the server](https://docs.livekit.io/transport/self-hosting/) where both machines can reach it. Each participant needs its own `--identity`.

For a web viewer on the operator side, run `rerun --serve-web` there and pass its proxy address as `--viewer`.

## How data travels

The robot side looks at each chunk and picks one of three lanes:

| Lane | What | LiveKit primitive | When the link is slow |
|---|---|---|---|
| Guaranteed | Blueprints, static data, and temporal `Clear`, `TextLog`, `TextDocument`, `StateChange`, `VideoFrameReference`, `VideoStream` | Byte streams | Queues up instead of dropping. What is still queued is lost if the session restarts. |
| Latest | All other temporal data | A data track, one frame per burst of logging | Old messages are dropped. |
| Video | 8-bit `Image` (L, RGB, RGBA, BGR, BGRA) and JPEG or PNG `EncodedImage` | One video track per image stream | Bitrate and resolution drop. |

Chunks on the guaranteed and latest lanes are forwarded as they are: the bridge reads them to route them, but never rewrites them. Images on the video lane are rebuilt on the operator side with the same entity path, row id, times, resolution and colour model.

Latest-wins data is dropped at the robot when it is older than `--max-age-millis` (one second by default), because for teleoperation stale data is worse than none.

## Choosing what to send

Pass a rules file to the robot side: `rerun-livekit robot --rules crates/rerun_livekit/example-rules.toml`.

```toml
[[rule]]
entities = "/debug/**"
drop = true

[[rule]]
entities = "/camera/wrist/**"
max_hz = 10
```

The first rule that matches an entity decides. See [`example-rules.toml`](crates/rerun_livekit/example-rules.toml) for every option: `drop`, `max_hz`, `video` and `delivery`.

Rules only apply to temporal data. Blueprints and static data are always sent.

## Commands back to the robot

Control does not go through Rerun. [`control.rs`](crates/rerun_livekit/src/control.rs) carries **any bytes** as a command, so the two ends decide what a command is: a twist, joint targets, a gamepad state, a pose from a VR controller. It follows LiveKit's [teleoperation pattern](https://docs.livekit.io/robotics/teleop/):

- The driver sends the whole command many times a second on a lossy data track, so a lost command is replaced by the next one and never retried late.
- The robot hands out a lease with an RPC. Only the lease holder's commands count, and a second driver is refused.
- The robot stops when valid commands stop arriving. From then on another driver may take the lease.

The operator's input device needs this:

```rust
let mut driver = rerun_livekit::control::Driver::acquire(&room_args).await?;
loop {
    driver.send(my_command.to_bytes())?; // at a fixed rate, for example 30 Hz
    // …
}
```

And the robot needs this:

```rust
let command = rerun_livekit::control::receive_commands(room_args, Duration::from_millis(300));
loop {
    match command.borrow().as_deref() {
        Some(bytes) => { /* decode and follow */ }
        None => { /* nobody is driving, or commands stopped: stop */ }
    }
    // …
}
```

[`keyboard_teleop`](crates/rerun_livekit/examples/keyboard_teleop.rs) and [`sim_robot`](crates/rerun_livekit/examples/sim_robot.rs) are a working pair. Their command is a forward speed and a turn rate, as two little-endian `f32`.

## Limits

- **The first frames of a camera are lost** while its video track starts.
- **Alpha does not survive video.** `RGBA` and `BGRA` images come back opaque.
- **Other images are not video.** 16-bit, float and chroma-subsampled images, depth images, and images logged together with other components (such as opacity) travel unchanged on the latest lane.
- **Latest-wins data can be lost without any network loss.** LiveKit keeps only the newest unsent data track frame of a participant. The bridge sends everything logged together as one frame so that messages do not evict each other, but a frame can still be replaced by the next one while a large frame is going out.
- **A latest-wins message over about 1.9 MB is dropped**, with a warning. Raw images that are not video and dense point clouds can exceed that. Reduce them at the source, or mark them `delivery = "guaranteed"`.
- **`VideoStream` is forwarded as data**, on the guaranteed lane, without LiveKit's video transport.
- **Images arrive decoded**, so the operator's viewer holds raw frames in memory. The viewer's memory limit takes care of old ones.
- **Control has no clock.** A command is valid from when it arrives until the watchdog timeout. A command the network delays is not detected as old.

- **Use release builds.** A debug build takes tens of seconds to skip the proxy's history when the robot side starts. A release build takes about one.

## Measured

On one Apple Silicon laptop, with everything on localhost, release builds, and `sim_robot` (a 640x360 RGB camera and three small streams at 30 Hz):

| | Mean | Max |
|---|---|---|
| Latest-wins data, from chunk creation to the operator side | 5 ms | 12 ms |
| Video, from the log call to a decoded frame on the operator side | 13 ms | 20 ms |

- These numbers stop at the operator side of the bridge. They leave out the viewer's ingestion and drawing, and a real network.
- The operator side logs them every ten seconds. Across two machines they include the difference between the two clocks.
- `rerun-livekit operator --plot-latency` also plots them in the viewer, one series for data and one per camera, under `/livekit/latency_ms`. Each point sits at the times of the message it measures, so the plots line up with the data on every timeline. This is the one case where the bridge adds entities the robot did not log, so it is off by default.
- A camera loses about 0.65 s of frames while its video track starts.

## Testing

```sh
cargo test                     # routing, rules, frame metadata, lease and replay logic
python scripts/smoke_test.py   # the whole thing, on this machine
```

The smoke test needs `livekit-server` and `rerun` on the `PATH`, and Python with `rerun-sdk` and Pillow. It starts a room, both sides of the bridge and an app, then checks that the operator side receives the same recording, that images show the right frame at the right time, that a late joiner gets the scene, and that the control lease is exclusive and can be taken over.

## Layout

| File | What |
|---|---|
| [`src/robot.rs`](crates/rerun_livekit/src/robot.rs) | Reads the proxy, routes each message to a lane, replays to late joiners |
| [`src/classify.rs`](crates/rerun_livekit/src/classify.rs) | Which lane a message takes |
| [`src/rules.rs`](crates/rerun_livekit/src/rules.rs) | The rules file |
| [`src/video.rs`](crates/rerun_livekit/src/video.rs) | Images to video frames and back |
| [`src/operator.rs`](crates/rerun_livekit/src/operator.rs) | Receives the lanes and writes to the viewer |
| [`src/latency.rs`](crates/rerun_livekit/src/latency.rs) | Latency report and plots |
| [`src/control.rs`](crates/rerun_livekit/src/control.rs) | Commands, lease and watchdog |
| [`examples/sim_robot.rs`](crates/rerun_livekit/examples/sim_robot.rs) | A simulated robot to try it with |
| [`examples/keyboard_teleop.rs`](crates/rerun_livekit/examples/keyboard_teleop.rs) | Drives it from the keyboard |
| [`scripts/smoke_test.py`](scripts/smoke_test.py) | End-to-end check on one machine |
