"""Tests for pub_sub_kiss_icp.processor (KissICPProcessor)."""

import numpy as np
import pytest

from pub_sub_kiss_icp.config import KissICPConfig
from pub_sub_kiss_icp.processor import KissICPProcessor


def _random_scan(n: int = 500) -> np.ndarray:
    """Generate a random point cloud with points distributed around origin."""
    rng = np.random.default_rng(42)
    return rng.standard_normal((n, 3)).astype(np.float32)


class TestKissICPProcessor:
    def test_initial_state(self):
        proc = KissICPProcessor()
        assert proc.frame_count == 0
        np.testing.assert_array_equal(proc.current_pose, np.eye(4))

    def test_register_increments_frame_count(self):
        proc = KissICPProcessor()
        scan = _random_scan()
        proc.register_frame(scan)
        assert proc.frame_count == 1
        proc.register_frame(scan)
        assert proc.frame_count == 2

    def test_pose_is_4x4_after_frame(self):
        proc = KissICPProcessor()
        proc.register_frame(_random_scan())
        assert proc.current_pose.shape == (4, 4)

    def test_xyzi_input_accepted(self):
        proc = KissICPProcessor()
        scan_xyzi = np.random.rand(200, 4).astype(np.float32)
        proc.register_frame(scan_xyzi)
        assert proc.frame_count == 1

    def test_float64_input_accepted(self):
        proc = KissICPProcessor()
        scan_f64 = np.random.rand(200, 3)  # float64
        proc.register_frame(scan_f64)
        assert proc.frame_count == 1

    def test_timestamps_accepted(self):
        proc = KissICPProcessor()
        scan = _random_scan()
        ts = np.linspace(0.0, 1.0, len(scan))
        proc.register_frame(scan, timestamps=ts)
        assert proc.frame_count == 1

    def test_invalid_shape_raises(self):
        proc = KissICPProcessor()
        with pytest.raises(ValueError, match="shape"):
            proc.register_frame(np.zeros((10, 2)))

    def test_reset_clears_state(self):
        proc = KissICPProcessor()
        proc.register_frame(_random_scan())
        assert proc.frame_count == 1
        proc.reset()
        assert proc.frame_count == 0
        np.testing.assert_array_equal(proc.current_pose, np.eye(4))

    def test_custom_config_applied(self):
        cfg = KissICPConfig(max_range=50.0, voxel_size=0.5)
        proc = KissICPProcessor(config=cfg)
        proc.register_frame(_random_scan())
        assert proc.frame_count == 1

    def test_current_pose_is_copy(self):
        """Modifying the returned pose should not corrupt internal state."""
        proc = KissICPProcessor()
        proc.register_frame(_random_scan())
        pose_a = proc.current_pose
        pose_a[0, 3] = 999.0
        pose_b = proc.current_pose
        assert pose_b[0, 3] != 999.0
