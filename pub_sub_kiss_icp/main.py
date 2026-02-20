"""Command-line entry point for pub-sub-kiss-icp."""

from __future__ import annotations

import logging
import signal
import sys
from typing import Optional

import click

from pub_sub_kiss_icp.config import load_config
from pub_sub_kiss_icp.pipeline import create_pipeline_from_config


@click.command()
@click.option(
    "-c",
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True),
    help="Path to the YAML configuration file.",
)
@click.option(
    "-l",
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    show_default=True,
    help="Logging verbosity level.",
)
@click.option(
    "--max-frames",
    default=None,
    type=int,
    help="Stop after processing this many frames (default: run indefinitely).",
)
def main(config_path: str, log_level: str, max_frames: Optional[int]) -> None:
    """pub-sub-kiss-icp: Subscribe to point cloud frames, run KISS-ICP, publish odometry.

    Supported protocols: kafka, pulsar, eventhubs, iggy, googlepubsub, streamnative.

    Example:

        pub-sub-kiss-icp -c examples/mock-config.yaml -l DEBUG
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    config = load_config(config_path)
    pipeline = create_pipeline_from_config(config)

    def _handle_signal(sig, frame):
        logging.getLogger(__name__).info("Signal %s received, shutting down…", sig)
        pipeline.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    pipeline.run(max_frames=max_frames)
    sys.exit(0)


if __name__ == "__main__":
    main()
