#!/usr/bin/env python3
"""
Registers one teleoperated episode in a Rerun catalog, with both ends as layers of one segment.

The `robot` layer is the robot's recording: everything it logged. The `operator` layer holds what
only the operator's end knows: the camera images that reached it, and the latency plots of
`rerun-livekit operator --plot-latency`. The images move below `--operator-prefix`, so that they
sit next to the robot's instead of taking their place.

Run it with `pixi run register-episode robot.rrd operator.rrd`, while `rerun server` is running.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import rerun as rr
import rerun.blueprint as rrb
from rerun.chunk import LazyChunkStream, RrdReader

# What the bridge sends as video, which is where the operator's copy differs from the robot's.
VIDEO_COMPONENTS = ["Image:buffer", "Image:format", "EncodedImage:blob", "EncodedImage:media_type"]

# Where `rerun-livekit operator --plot-latency` plots.
LATENCY_PLOTS = "/livekit/**"

# Where the MJCF importer puts the robot of `mujoco_robot.py`.
ROBOT = "/g1\\ scene"


def write_operator_layer(operator_rrd: Path, layer_rrd: Path, prefix: str, recording_id: str) -> None:
    """Writes what only the operator's end knows to `layer_rrd`, as part of recording `recording_id`."""
    operator = RrdReader(operator_rrd)
    images = (
        operator.stream()
        .filter(components=VIDEO_COMPONENTS)
        .map(lambda chunk: chunk.with_entity_path(prefix + chunk.entity_path))
    )
    latency = operator.stream().filter(content=LATENCY_PLOTS)
    LazyChunkStream.merge(images, latency).write_rrd(
        layer_rrd, application_id=operator.recordings()[0].application_id, recording_id=recording_id
    )


def blueprint(prefix: str) -> rrb.Blueprint:
    """The robot next to what its camera recorded, what the operator saw of it, and how late that was."""
    return rrb.Blueprint(
        rrb.Horizontal(
            rrb.Spatial3DView(
                name="Robot",
                contents=["+ /**", f"- {prefix}/**", f"- {ROBOT}/collision_geometries/**"],
                eye_controls=rrb.EyeControls3D(
                    position=[-2.5, -2.0, 1.6],
                    look_target=[0.0, 0.0, 0.7],
                    tracking_entity=f"{ROBOT}/visual_geometries/torso_link/geom_0",
                ),
            ),
            rrb.Vertical(
                rrb.Horizontal(
                    rrb.Spatial2DView(name="Robot's camera", origin="/camera"),
                    rrb.Spatial2DView(name="Operator's view", origin=f"{prefix}/camera"),
                ),
                rrb.TimeSeriesView(name="Latency (ms)", origin="/livekit/latency_ms"),
                rrb.TimeSeriesView(name="Forward velocity", origin="/velocity"),
            ),
        ),
        collapse_panels=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("robot_rrd", type=Path, help="What the robot recorded, for example with `--save`.")
    parser.add_argument("operator_rrd", type=Path, help="What the operator's Viewer received.")
    parser.add_argument("--catalog", default="rerun+http://127.0.0.1:51234", help="The catalog to register in.")
    parser.add_argument("--dataset", default="teleop", help="The dataset the episode becomes a segment of.")
    parser.add_argument("--operator-prefix", default="/operator", help="Where the operator's images go.")
    args = parser.parse_args()

    # A segment is keyed by its recording id, which the bridge keeps the same on both ends.
    episode = RrdReader(args.robot_rrd).recordings()[0].recording_id
    if RrdReader(args.operator_rrd).recordings()[0].recording_id != episode:
        sys.exit(f"{args.operator_rrd} is not a recording of episode {episode!r}")

    dataset = rr.catalog.CatalogClient(args.catalog).create_dataset(args.dataset, exist_ok=True)
    dataset.register(args.robot_rrd.resolve().as_uri(), layer_name="robot").wait()

    layer_rrd = args.operator_rrd.with_suffix(".layer.rrd")
    write_operator_layer(args.operator_rrd, layer_rrd, args.operator_prefix, episode)
    dataset.register(layer_rrd.resolve().as_uri(), layer_name="operator").wait()

    if dataset.default_blueprint() is None:
        blueprint_rbl = args.operator_rrd.with_suffix(".rbl")
        blueprint(args.operator_prefix).save(args.dataset, blueprint_rbl)
        dataset.register_blueprint(blueprint_rbl.resolve().as_uri(), name="both ends")

    print(f"Registered {episode} in {args.dataset}: {dataset.segment_url(episode)}")


if __name__ == "__main__":
    main()
