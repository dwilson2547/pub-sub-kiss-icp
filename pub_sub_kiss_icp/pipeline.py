"""Main processing pipeline.

The pipeline:
  1. Receives serialised point cloud frames from the *subscriber*.
  2. Deserialises each frame with :mod:`pub_sub_kiss_icp.serialization`.
  3. Feeds the frame into :class:`~pub_sub_kiss_icp.processor.KissICPProcessor`.
  4. Serialises the resulting odometry estimate.
  5. Publishes the odometry via the *publisher*.

The loop runs until :meth:`KissICPPipeline.stop` is called (e.g. from a
signal handler) or *max_frames* frames have been processed.
"""

from __future__ import annotations

import logging
import threading
from typing import Optional

from pub_sub_kiss_icp.adapters.base import Publisher, Subscriber
from pub_sub_kiss_icp.config import KissICPConfig, PipelineConfig
from pub_sub_kiss_icp.processor import KissICPProcessor
from pub_sub_kiss_icp.serialization import (
    deserialize_pointcloud,
    serialize_odometry,
)

logger = logging.getLogger(__name__)


class KissICPPipeline:
    """Pub-sub wrapper around KISS-ICP.

    Args:
        subscriber: Connected (or not yet connected) subscriber that yields
                    serialised point cloud frames.
        publisher: Connected (or not yet connected) publisher that accepts
                   serialised odometry messages.
        kiss_icp_config: KISS-ICP algorithm parameters.
        poll_timeout_ms: How long to wait for a new message on each iteration.
    """

    def __init__(
        self,
        subscriber: Subscriber,
        publisher: Publisher,
        kiss_icp_config: Optional[KissICPConfig] = None,
        poll_timeout_ms: int = 1000,
    ) -> None:
        self._subscriber = subscriber
        self._publisher = publisher
        self._processor = KissICPProcessor(config=kiss_icp_config)
        self._poll_timeout_ms = poll_timeout_ms
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def stop(self) -> None:
        """Signal the pipeline loop to exit after the current frame."""
        self._stop_event.set()

    @property
    def frames_processed(self) -> int:
        """Total number of frames successfully processed."""
        return self._processor.frame_count

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self, max_frames: Optional[int] = None) -> None:
        """Start the subscribe → process → publish loop.

        Args:
            max_frames: Stop after processing this many frames.  Run
                        indefinitely when *None*.
        """
        logger.info("Starting KISS-ICP pipeline")
        with self._subscriber, self._publisher:
            while not self._stop_event.is_set():
                if max_frames is not None and self._processor.frame_count >= max_frames:
                    logger.info("Reached max_frames=%d, stopping", max_frames)
                    break

                raw = self._subscriber.receive(timeout_ms=self._poll_timeout_ms)
                if raw is None:
                    logger.debug("No message received within timeout, polling again")
                    continue

                self._process_message(raw)

        logger.info(
            "Pipeline stopped after processing %d frames", self._processor.frame_count
        )

    def _process_message(self, raw: bytes) -> None:
        """Deserialise, register, serialise, and publish a single frame."""
        try:
            points, timestamp = deserialize_pointcloud(raw)
        except Exception as exc:
            logger.error("Failed to deserialise point cloud: %s", exc)
            return

        try:
            self._processor.register_frame(points, timestamps=None)
        except Exception as exc:
            logger.error("KISS-ICP registration failed: %s", exc)
            return

        pose = self._processor.current_pose
        odometry = serialize_odometry(
            pose,
            frame_id=self._processor.frame_count,
            timestamp=timestamp,
        )

        try:
            self._publisher.publish(odometry)
            self._subscriber.acknowledge(None)
            logger.debug(
                "Published odometry for frame %d (timestamp=%.3f)",
                self._processor.frame_count,
                timestamp,
            )
        except Exception as exc:
            logger.error("Failed to publish odometry: %s", exc)


def create_pipeline_from_config(config: PipelineConfig) -> "KissICPPipeline":
    """Instantiate a :class:`KissICPPipeline` from a :class:`PipelineConfig`.

    This is the main factory function used by the CLI.
    """
    from pub_sub_kiss_icp.adapters import get_publisher_class, get_subscriber_class

    SubscriberCls = get_subscriber_class(config.source.type)
    PublisherCls = get_publisher_class(config.destination.type)

    subscriber = SubscriberCls(
        topic=config.source.topic,
        connection=dict(config.source.connection),
    )
    publisher = PublisherCls(
        topic=config.destination.topic,
        connection=dict(config.destination.connection),
    )

    return KissICPPipeline(
        subscriber=subscriber,
        publisher=publisher,
        kiss_icp_config=config.kiss_icp,
        poll_timeout_ms=config.poll_timeout_ms,
    )
