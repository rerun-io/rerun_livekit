#!/usr/bin/env python3
"""
End-to-end check of the bridge on one machine.

Starts a LiveKit dev server, Rerun proxies and viewers, and both sides of the bridge, then checks
that what an app logs on the robot side arrives on the operator side, that a late joiner gets
the scene, and that the control lease behaves.

Needs `livekit-server` and `rerun` on the PATH, and Python with `rerun-sdk` and Pillow.
Run it from anywhere: `python scripts/smoke_test.py`. It exits non-zero on the first failure.
"""

from __future__ import annotations

import io
import os
import pty
import select
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import numpy as np
import rerun as rr
import rerun.blueprint as rrb
from PIL import Image

REPO = Path(__file__).resolve().parent.parent
BRIDGE = REPO / "target/release/rerun-livekit"
SIM_ROBOT = REPO / "target/release/examples/sim_robot"
KEYBOARD_TELEOP = REPO / "target/release/examples/keyboard_teleop"

# Away from Rerun's default port, so a viewer that is already open is left alone.
ROBOT_PROXY_PORT = 19876
OPERATOR_PORT = 19877
LATE_OPERATOR_PORT = 19878

NUM_STEPS = 150
STEP_SECS = 0.05
IMAGE_SIZE = (160, 120)
JPEG_SIZE = (640, 480)

# A video track takes a moment to start, and its first frames are lost.
MAX_FRAMES_LOST = 40

STATIC_ENTITIES = {"/world/box", "/world/points", "/__properties"}
TEMPORAL_ENTITIES = {"/world/robot", "/signal", "/camera/image", "/camera/jpeg", "/log"}


class Processes:
    """Starts background processes and stops them all at the end."""

    def __init__(self, log_dir: Path) -> None:
        self.log_dir = log_dir
        self.running: list[subprocess.Popen[bytes]] = []

    def start(self, name: str, *command: object) -> subprocess.Popen[bytes]:
        """Starts `command` in the background, logging to a file called `name`."""
        log = (self.log_dir / f"{name}.log").open("wb")
        # In its own process group, because `rerun` may be a wrapper script around the real
        # binary, and stopping only the wrapper would leave the binary running.
        process = subprocess.Popen(
            [str(arg) for arg in command], stdout=log, stderr=subprocess.STDOUT, start_new_session=True
        )
        self.running.append(process)
        return process

    def stop(self, process: subprocess.Popen[bytes]) -> None:
        """Interrupts `process`, and kills it if that is not enough."""
        os.killpg(process.pid, signal.SIGINT)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
        self.running.remove(process)

    def stop_all(self) -> None:
        """Stops everything that is still running."""
        for process in list(self.running):
            self.stop(process)


def proxy_url(port: int) -> str:
    return f"rerun+http://127.0.0.1:{port}/proxy"


def check(condition: bool, message: str) -> None:
    check_quietly(condition, message)
    print(f"  ok: {message}")


def check_quietly(condition: bool, message: str) -> None:
    """Like `check`, for checks that run once per row."""
    if not condition:
        sys.exit(f"FAILED: {message}")


def stripe_column(step: int, width: int) -> int:
    return (step * 3) % width


def log_app() -> str:
    """Logs like a robot app would, with the stock SDK. Returns the recording id."""
    rec = rr.RecordingStream("rerun_example_livekit_smoke_test")
    rec.connect_grpc(proxy_url(ROBOT_PROXY_PORT))
    rec.send_blueprint(rrb.Horizontal(rrb.Spatial3DView(origin="/world"), rrb.Spatial2DView(origin="/camera")))
    rec.log("world/box", rr.Boxes3D(half_sizes=[1, 1, 1]), static=True)
    rec.log("world/points", rr.Points3D(np.random.rand(100, 3)), static=True)

    for step in range(NUM_STEPS):
        rec.set_time("step", sequence=step)
        rec.log("world/robot", rr.Transform3D(translation=[np.sin(step / 10), 0, 0]))
        rec.log("signal", rr.Scalars(np.sin(step / 10)))

        width, height = IMAGE_SIZE
        image = np.zeros((height, width, 3), dtype=np.uint8)
        image[:, stripe_column(step, width)] = 255
        rec.log("camera/image", rr.Image(image))

        width, height = JPEG_SIZE
        image = np.zeros((height, width, 3), dtype=np.uint8)
        image[:, stripe_column(step, width)] = (0, 255, 0)
        rec.log("camera/jpeg", rr.Image(image).compress(jpeg_quality=80))

        time.sleep(STEP_SECS)

    rec.log("log", rr.TextLog("done"))
    rec.flush()
    recording_id = str(rec.get_recording_id())
    rec.disconnect()
    return recording_id


def read_rows(rrd: Path) -> dict[str, list[dict[str, Any]]]:
    """Returns the rows of each entity in the recording saved at `rrd`, as column name to value."""
    rows: dict[str, list[dict[str, Any]]] = {}
    for chunk in rr.chunk.RrdReader(rrd).stream().to_chunks():
        batch = chunk.to_record_batch()
        rows.setdefault(chunk.entity_path, []).extend(batch.to_pylist())
    return rows


def brightest_column(pixels: np.ndarray, channel: int) -> int:
    return int(pixels[:, :, channel].mean(axis=0).argmax())


def check_stream(rrd: Path, recording_id: str) -> None:
    reader = rr.chunk.RrdReader(rrd)
    check([store.recording_id for store in reader.recordings()] == [recording_id], "the recording keeps its id")
    check(len(reader.blueprints()) == 1, "the blueprint arrived")

    rows = read_rows(rrd)
    check(
        set(rows) == STATIC_ENTITIES | TEMPORAL_ENTITIES,
        f"the operator side has exactly the logged entities: {sorted(rows)}",
    )
    for entity in ["/world/robot", "/signal"]:
        missing = sorted(set(range(NUM_STEPS)) - {row["step"] for row in rows[entity]})
        check(not missing, f"all {NUM_STEPS} rows of {entity} arrived (missing steps: {missing})")
    check(len(rows["/log"]) == 1, "the text log arrived")

    images = rows["/camera/image"]
    check(NUM_STEPS - MAX_FRAMES_LOST <= len(images), f"{len(images)} of {NUM_STEPS} images arrived")
    width, height = IMAGE_SIZE
    for row in images:
        check_quietly(row["Image:format"][0]["width"] == width, "an image changed size")
        pixels = np.asarray(row["Image:buffer"][0], dtype=np.uint8).reshape(height, width, 3)
        check_quietly(
            brightest_column(pixels, 0) == stripe_column(row["step"], width),
            f"image at step {row['step']} shows another step",
        )
    print("  ok: every image shows the stripe of its own step")

    jpegs = rows["/camera/jpeg"]
    check(NUM_STEPS - MAX_FRAMES_LOST <= len(jpegs), f"{len(jpegs)} of {NUM_STEPS} encoded images arrived")
    for row in jpegs:
        check_quietly(row["EncodedImage:media_type"] == ["image/jpeg"], "an encoded image lost its media type")
        pixels = np.asarray(Image.open(io.BytesIO(bytes(row["EncodedImage:blob"][0]))))
        check_quietly(pixels.shape[:2] == JPEG_SIZE[::-1], "an encoded image changed size")
        check_quietly(
            abs(brightest_column(pixels, 1) - stripe_column(row["step"], JPEG_SIZE[0])) <= 1,
            f"encoded image at step {row['step']} shows another step",
        )
    print("  ok: every encoded image shows the stripe of its own step")


def check_late_join(rrd: Path) -> None:
    check(len(rr.chunk.RrdReader(rrd).blueprints()) == 1, "a late joiner gets the blueprint")
    rows = read_rows(rrd)
    check(STATIC_ENTITIES <= set(rows), "a late joiner gets the static data")
    late_temporal = TEMPORAL_ENTITIES & set(rows)
    check(not late_temporal, f"a late joiner gets no old temporal data: {sorted(late_temporal)}")


def start_driver(identity: str) -> tuple[int, int]:
    """Starts `keyboard_teleop` in a pseudo-terminal, which it needs to read keys."""
    pid, terminal = pty.fork()
    if pid == 0:
        os.execv(KEYBOARD_TELEOP, [str(KEYBOARD_TELEOP), "--identity", identity])
    return pid, terminal


def read_terminal(terminal: int, secs: float) -> str:
    output = b""
    deadline = time.time() + secs
    while time.time() < deadline:
        readable, _, _ = select.select([terminal], [], [], 0.1)
        if readable:
            try:
                output += os.read(terminal, 4096)
            except OSError:
                break
    return output.decode(errors="replace")


def check_control() -> None:
    alice, alice_terminal = start_driver("alice")
    check("You are driving" in read_terminal(alice_terminal, 5), "the first driver gets the lease")
    os.write(alice_terminal, b"ww")

    bob, bob_terminal = start_driver("bob")
    check("is driving" in read_terminal(bob_terminal, 5), "a second driver is refused while the first drives")
    os.waitpid(bob, 0)

    os.kill(alice, signal.SIGKILL)
    os.waitpid(alice, 0)
    time.sleep(1)
    carol, carol_terminal = start_driver("carol")
    check("You are driving" in read_terminal(carol_terminal, 5), "another driver takes over after the first one dies")
    os.write(carol_terminal, b"q")
    os.waitpid(carol, 0)


def main() -> None:
    subprocess.run(["cargo", "build", "--release", "--all-targets"], cwd=REPO, check=True)

    with tempfile.TemporaryDirectory(prefix="rerun-livekit-smoke-") as temp_dir:
        temp = Path(temp_dir)
        processes = Processes(temp)
        try:
            run(processes, temp)
        except SystemExit:
            for log in sorted(temp.glob("*.log")):
                lines = log.read_text(errors="replace").splitlines()
                ours = [line for line in lines if "rerun_livekit" in line or "ERROR" in line]
                print(f"--- {log.name}", *ours[-15:], sep="\n")
            raise
        finally:
            processes.stop_all()
    print("PASSED")


def run(processes: Processes, temp: Path) -> None:
    operator_rrd = temp / "operator.rrd"
    late_rrd = temp / "late.rrd"

    processes.start("livekit", "livekit-server", "--dev")
    processes.start("robot_proxy", "rerun", "--serve-grpc", "--port", ROBOT_PROXY_PORT)
    viewer = processes.start("viewer", "rerun", "--headless", "--port", OPERATOR_PORT, "--save", operator_rrd)
    time.sleep(3)
    processes.start("robot", BRIDGE, "robot", "--proxy", proxy_url(ROBOT_PROXY_PORT))
    operator = processes.start("operator", BRIDGE, "operator", "--viewer", proxy_url(OPERATOR_PORT))
    time.sleep(4)

    print("Stream")
    recording_id = log_app()
    time.sleep(3)
    processes.stop(operator)
    processes.stop(viewer)
    check_stream(operator_rrd, recording_id)

    print("Late join")
    late_viewer = processes.start(
        "late_viewer", "rerun", "--headless", "--port", LATE_OPERATOR_PORT, "--save", late_rrd
    )
    time.sleep(3)
    late_operator = processes.start(
        "late_operator", BRIDGE, "operator", "--identity", "late", "--viewer", proxy_url(LATE_OPERATOR_PORT)
    )
    time.sleep(6)
    processes.stop(late_operator)
    processes.stop(late_viewer)
    check_late_join(late_rrd)

    print("Control")
    processes.start("sim_robot", SIM_ROBOT, "--proxy", proxy_url(ROBOT_PROXY_PORT))
    time.sleep(4)
    check_control()


if __name__ == "__main__":
    main()
