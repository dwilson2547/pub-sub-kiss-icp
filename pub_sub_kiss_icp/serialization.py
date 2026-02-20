"""Serialization helpers for point clouds and odometry messages.

Point cloud wire format (binary):
  - 8 bytes  : magic b'KISSPCL\\x01'
  - 4 bytes  : num_points  (uint32, big-endian)
  - 4 bytes  : point_dim   (uint32, 3 or 4 – XYZ or XYZI)
  - 8 bytes  : timestamp   (float64, seconds since epoch; 0.0 if unavailable)
  - N*dim*4  : float32 point data (row-major)

Odometry wire format: UTF-8 encoded JSON, schema:
  {
    "frame_id": <int>,
    "timestamp": <float>,
    "pose": [[4 x 4 float matrix]]
  }
"""

from __future__ import annotations

import json
import struct
from typing import Optional, Tuple

import numpy as np

_MAGIC = b"KISSPCL\x01"
_HEADER_FMT = ">II d"  # num_points, point_dim, timestamp
_HEADER_SIZE = struct.calcsize(_HEADER_FMT)  # 16 bytes


# ---------------------------------------------------------------------------
# Point cloud
# ---------------------------------------------------------------------------


def serialize_pointcloud(
    points: np.ndarray,
    timestamp: float = 0.0,
) -> bytes:
    """Serialize a point cloud to bytes.

    Args:
        points: NumPy array of shape (N, 3) or (N, 4), dtype float32 or float64.
        timestamp: Frame acquisition time in seconds since epoch.

    Returns:
        Serialised byte string.
    """
    if points.ndim != 2 or points.shape[1] not in (3, 4):
        raise ValueError(
            f"points must have shape (N, 3) or (N, 4), got {points.shape}"
        )
    pts_f32 = np.asarray(points, dtype=np.float32)
    num_points, point_dim = pts_f32.shape
    header = _MAGIC + struct.pack(_HEADER_FMT, num_points, point_dim, timestamp)
    return header + pts_f32.tobytes()


def deserialize_pointcloud(data: bytes) -> Tuple[np.ndarray, float]:
    """Deserialise a point cloud message.

    Returns:
        (points, timestamp) where points has shape (N, dim) as float32 and
        timestamp is a float seconds-since-epoch value.
    """
    magic_len = len(_MAGIC)
    if len(data) < magic_len + _HEADER_SIZE:
        raise ValueError("Message too short to be a valid point cloud")
    if data[:magic_len] != _MAGIC:
        raise ValueError("Invalid magic bytes; not a KISSPCL message")
    num_points, point_dim, timestamp = struct.unpack(
        _HEADER_FMT, data[magic_len : magic_len + _HEADER_SIZE]
    )
    payload = data[magic_len + _HEADER_SIZE :]
    expected = num_points * point_dim * 4
    if len(payload) != expected:
        raise ValueError(
            f"Payload length mismatch: expected {expected} bytes, got {len(payload)}"
        )
    points = np.frombuffer(payload, dtype=np.float32).reshape(num_points, point_dim)
    return points, float(timestamp)


# ---------------------------------------------------------------------------
# Odometry
# ---------------------------------------------------------------------------


def serialize_odometry(
    pose: np.ndarray,
    frame_id: int,
    timestamp: float = 0.0,
) -> bytes:
    """Serialise a 4×4 pose matrix to a UTF-8 JSON byte string.

    Args:
        pose: 4×4 NumPy array representing the sensor pose in world frame.
        frame_id: Sequential frame counter.
        timestamp: Timestamp of the corresponding point cloud frame.

    Returns:
        UTF-8 encoded JSON bytes.
    """
    if pose.shape != (4, 4):
        raise ValueError(f"pose must be a 4×4 matrix, got {pose.shape}")
    payload = {
        "frame_id": frame_id,
        "timestamp": timestamp,
        "pose": pose.tolist(),
    }
    return json.dumps(payload).encode("utf-8")


def deserialize_odometry(data: bytes) -> dict:
    """Deserialise an odometry message into a plain dict."""
    return json.loads(data.decode("utf-8"))
