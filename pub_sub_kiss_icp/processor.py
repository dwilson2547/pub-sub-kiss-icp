"""KISS-ICP processor wrapper.

Thin adapter around :class:`kiss_icp.kiss_icp.KissICP` that accepts raw numpy
point clouds, manages the KISS-ICP instance lifecycle, and exposes the current
pose after each frame registration.
"""

from __future__ import annotations

from typing import Optional, Tuple

import numpy as np

from kiss_icp.config import KISSConfig, load_config as _kiss_load_config
from kiss_icp.kiss_icp import KissICP

from pub_sub_kiss_icp.config import KissICPConfig


def _build_kiss_config(cfg: KissICPConfig) -> KISSConfig:
    """Convert our config dataclass into a KISSConfig pydantic model."""
    kiss_cfg = KISSConfig()
    kiss_cfg.data.max_range = cfg.max_range
    kiss_cfg.data.min_range = cfg.min_range
    kiss_cfg.data.deskew = cfg.deskew
    kiss_cfg.mapping.max_points_per_voxel = cfg.max_points_per_voxel
    kiss_cfg.adaptive_threshold.initial_threshold = cfg.initial_threshold
    kiss_cfg.adaptive_threshold.min_motion_th = cfg.min_motion_th
    # Replicate KISS-ICP's own logic: auto-compute voxel_size from max_range when unset
    kiss_cfg.mapping.voxel_size = (
        cfg.voxel_size if cfg.voxel_size is not None else float(cfg.max_range / 100.0)
    )
    return kiss_cfg


class KissICPProcessor:
    """Stateful processor that registers successive point cloud frames.

    Each call to :meth:`register_frame` feeds the next point cloud into
    KISS-ICP and returns the updated 4×4 sensor pose in world frame.

    Args:
        config: Algorithm parameters.  A default :class:`~pub_sub_kiss_icp.config.KissICPConfig`
                is used when *None* is supplied.
    """

    def __init__(self, config: Optional[KissICPConfig] = None) -> None:
        self._config = config or KissICPConfig()
        self._kiss_icp = KissICP(config=_build_kiss_config(self._config))
        self._frame_count: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def frame_count(self) -> int:
        """Number of frames registered so far."""
        return self._frame_count

    @property
    def current_pose(self) -> np.ndarray:
        """Current 4×4 sensor pose in world frame (identity before first frame)."""
        return self._kiss_icp.last_pose.copy()

    def register_frame(
        self,
        points: np.ndarray,
        timestamps: Optional[np.ndarray] = None,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Register a new point cloud frame.

        Args:
            points: Array of shape (N, 3) or (N, 4) in float32 or float64.
                    Only the first three columns (XYZ) are used by KISS-ICP;
                    a fourth column (intensity) is silently ignored.
            timestamps: Optional per-point timestamps of shape (N,) in float64,
                        used for motion de-skewing.  Defaults to linearly
                        spaced values in [0, 1] when *None*.

        Returns:
            Tuple of *(deskewed_frame, source_points)* – the preprocessed scan
            and the voxelised points used for ICP registration.
        """
        pts = np.asarray(points, dtype=np.float64)
        if pts.ndim != 2 or pts.shape[1] not in (3, 4):
            raise ValueError(
                f"points must have shape (N, 3) or (N, 4), got {pts.shape}"
            )
        # KISS-ICP only uses XYZ
        xyz = pts[:, :3]

        if timestamps is None:
            ts = np.linspace(0.0, 1.0, len(xyz))
        else:
            ts = np.asarray(timestamps, dtype=np.float64)

        frame, source = self._kiss_icp.register_frame(xyz, ts)
        self._frame_count += 1
        return frame, source

    def reset(self) -> None:
        """Reset the processor to its initial state (clears the local map)."""
        self._kiss_icp = KissICP(config=_build_kiss_config(self._config))
        self._frame_count = 0
