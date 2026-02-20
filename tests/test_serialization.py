"""Tests for pub_sub_kiss_icp.serialization."""

import json
import struct

import numpy as np
import pytest

from pub_sub_kiss_icp.serialization import (
    _MAGIC,
    deserialize_odometry,
    deserialize_pointcloud,
    serialize_odometry,
    serialize_pointcloud,
)


class TestSerializePointcloud:
    def test_roundtrip_xyz(self):
        pts = np.random.rand(100, 3).astype(np.float32)
        data = serialize_pointcloud(pts, timestamp=1.23)
        recovered, ts = deserialize_pointcloud(data)
        np.testing.assert_array_almost_equal(recovered, pts)
        assert abs(ts - 1.23) < 1e-6

    def test_roundtrip_xyzi(self):
        pts = np.random.rand(50, 4).astype(np.float32)
        data = serialize_pointcloud(pts, timestamp=0.0)
        recovered, ts = deserialize_pointcloud(data)
        np.testing.assert_array_almost_equal(recovered, pts)
        assert ts == 0.0

    def test_float64_input_is_cast(self):
        pts = np.random.rand(20, 3)  # float64
        data = serialize_pointcloud(pts)
        recovered, _ = deserialize_pointcloud(data)
        # Result should be float32
        assert recovered.dtype == np.float32
        # Values should be close after cast
        np.testing.assert_array_almost_equal(recovered, pts.astype(np.float32))

    def test_invalid_shape_raises(self):
        with pytest.raises(ValueError, match="shape"):
            serialize_pointcloud(np.zeros((10, 2)))

    def test_bad_magic_raises(self):
        pts = np.zeros((5, 3), dtype=np.float32)
        data = bytearray(serialize_pointcloud(pts))
        data[0] = 0xFF  # corrupt magic
        with pytest.raises(ValueError, match="magic"):
            deserialize_pointcloud(bytes(data))

    def test_too_short_raises(self):
        with pytest.raises(ValueError, match="too short"):
            deserialize_pointcloud(b"short")

    def test_payload_mismatch_raises(self):
        pts = np.zeros((10, 3), dtype=np.float32)
        data = bytearray(serialize_pointcloud(pts))
        # Truncate payload
        truncated = bytes(data[:-4])
        with pytest.raises(ValueError, match="mismatch"):
            deserialize_pointcloud(truncated)


class TestSerializeOdometry:
    def test_roundtrip(self):
        pose = np.eye(4)
        pose[0, 3] = 1.5
        data = serialize_odometry(pose, frame_id=7, timestamp=99.9)
        result = deserialize_odometry(data)
        assert result["frame_id"] == 7
        assert abs(result["timestamp"] - 99.9) < 1e-9
        recovered = np.array(result["pose"])
        np.testing.assert_array_almost_equal(recovered, pose)

    def test_output_is_valid_json(self):
        pose = np.eye(4)
        data = serialize_odometry(pose, frame_id=0)
        parsed = json.loads(data.decode("utf-8"))
        assert "pose" in parsed
        assert "frame_id" in parsed

    def test_invalid_shape_raises(self):
        with pytest.raises(ValueError, match="4×4"):
            serialize_odometry(np.eye(3), frame_id=0)
