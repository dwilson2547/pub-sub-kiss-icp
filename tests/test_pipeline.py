"""Integration-style tests for the KissICPPipeline using the mock adapter."""

import json

import numpy as np
import pytest

from pub_sub_kiss_icp.adapters.mock import MockPublisher, MockSubscriber
from pub_sub_kiss_icp.config import KissICPConfig
from pub_sub_kiss_icp.pipeline import KissICPPipeline
from pub_sub_kiss_icp.serialization import (
    deserialize_odometry,
    serialize_pointcloud,
)


def _make_scan(n: int = 300, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.standard_normal((n, 3)).astype(np.float32)


def _enqueue_frames(subscriber: MockSubscriber, num_frames: int) -> None:
    for i in range(num_frames):
        scan = _make_scan(seed=i)
        payload = serialize_pointcloud(scan, timestamp=float(i))
        subscriber.push(payload)


class TestKissICPPipeline:
    def _make_pipeline(self, num_frames: int = 3) -> tuple:
        sub = MockSubscriber()
        pub = MockPublisher()
        _enqueue_frames(sub, num_frames)
        pipeline = KissICPPipeline(
            subscriber=sub,
            publisher=pub,
            kiss_icp_config=KissICPConfig(),
        )
        return pipeline, sub, pub

    def test_processes_frames(self):
        pipeline, _, pub = self._make_pipeline(num_frames=3)
        pipeline.run(max_frames=3)
        assert pipeline.frames_processed == 3
        assert len(pub.messages) == 3

    def test_odometry_output_is_valid_json(self):
        pipeline, _, pub = self._make_pipeline(num_frames=1)
        pipeline.run(max_frames=1)
        assert len(pub.messages) == 1
        result = deserialize_odometry(pub.messages[0])
        assert "frame_id" in result
        assert "pose" in result
        assert len(result["pose"]) == 4

    def test_frame_id_increments(self):
        pipeline, _, pub = self._make_pipeline(num_frames=3)
        pipeline.run(max_frames=3)
        frame_ids = [deserialize_odometry(m)["frame_id"] for m in pub.messages]
        assert frame_ids == [1, 2, 3]

    def test_timestamp_preserved(self):
        pipeline, _, pub = self._make_pipeline(num_frames=2)
        pipeline.run(max_frames=2)
        ts_0 = deserialize_odometry(pub.messages[0])["timestamp"]
        ts_1 = deserialize_odometry(pub.messages[1])["timestamp"]
        assert abs(ts_0 - 0.0) < 1e-6
        assert abs(ts_1 - 1.0) < 1e-6

    def test_stop_halts_loop(self):
        sub = MockSubscriber()
        pub = MockPublisher()
        # Pre-load 10 frames but stop after 2
        _enqueue_frames(sub, 10)
        pipeline = KissICPPipeline(subscriber=sub, publisher=pub)
        pipeline.run(max_frames=2)
        assert pipeline.frames_processed == 2

    def test_bad_message_skipped(self):
        sub = MockSubscriber(messages=[b"garbage"])
        pub = MockPublisher()
        # One valid frame after the bad one
        _enqueue_frames(sub, 1)
        pipeline = KissICPPipeline(subscriber=sub, publisher=pub)
        pipeline.run(max_frames=1)
        # The garbage message is skipped; the valid frame is processed
        assert pipeline.frames_processed == 1
        assert len(pub.messages) == 1

    def test_empty_queue_stops_cleanly(self):
        sub = MockSubscriber()  # no messages
        pub = MockPublisher()
        pipeline = KissICPPipeline(
            subscriber=sub,
            publisher=pub,
            poll_timeout_ms=10,
        )
        pipeline.run(max_frames=0)
        assert pipeline.frames_processed == 0

    def test_context_manager_connects_and_closes(self):
        sub = MockSubscriber()
        pub = MockPublisher()
        _enqueue_frames(sub, 1)
        pipeline = KissICPPipeline(subscriber=sub, publisher=pub)
        # Context manager is used internally by run(); verify lifecycle manually
        with sub, pub:
            assert sub._connected
            assert pub._connected
        assert not sub._connected
        assert not pub._connected


class TestAdapterRegistry:
    def test_known_protocols_resolve(self):
        from pub_sub_kiss_icp.adapters import get_publisher_class, get_subscriber_class

        for proto in ("kafka", "pulsar", "streamnative", "eventhubs", "iggy", "googlepubsub", "mock"):
            sub_cls = get_subscriber_class(proto)
            pub_cls = get_publisher_class(proto)
            assert sub_cls is not None
            assert pub_cls is not None

    def test_unknown_protocol_raises(self):
        from pub_sub_kiss_icp.adapters import get_subscriber_class

        with pytest.raises(ValueError, match="Unknown protocol"):
            get_subscriber_class("mqtt")
