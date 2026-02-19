"""Configuration loading and validation for pub-sub-kiss-icp."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import yaml


@dataclass
class KissICPConfig:
    """KISS-ICP algorithm parameters."""

    max_range: float = 100.0
    min_range: float = 0.0
    voxel_size: Optional[float] = None
    deskew: bool = True
    max_points_per_voxel: int = 20
    initial_threshold: float = 2.0
    min_motion_th: float = 0.1


@dataclass
class AdapterConfig:
    """Connection and topic settings for a single pub-sub endpoint."""

    type: str  # kafka | pulsar | eventhubs | iggy | googlepubsub | streamnative | mock
    topic: str
    connection: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineConfig:
    """Top-level configuration for the pipeline."""

    source: AdapterConfig
    destination: AdapterConfig
    kiss_icp: KissICPConfig = field(default_factory=KissICPConfig)
    # Number of worker threads in the processing loop
    poll_timeout_ms: int = 1000


def _load_adapter(raw: Dict[str, Any]) -> AdapterConfig:
    return AdapterConfig(
        type=raw["type"],
        topic=raw["topic"],
        connection=raw.get("connection", {}),
    )


def _load_kiss_icp(raw: Dict[str, Any]) -> KissICPConfig:
    return KissICPConfig(
        max_range=raw.get("max_range", 100.0),
        min_range=raw.get("min_range", 0.0),
        voxel_size=raw.get("voxel_size", None),
        deskew=raw.get("deskew", True),
        max_points_per_voxel=raw.get("max_points_per_voxel", 20),
        initial_threshold=raw.get("initial_threshold", 2.0),
        min_motion_th=raw.get("min_motion_th", 0.1),
    )


def load_config(path: str) -> PipelineConfig:
    """Load a YAML configuration file and return a PipelineConfig."""
    with open(path, "r") as fh:
        raw = yaml.safe_load(fh)

    return PipelineConfig(
        source=_load_adapter(raw["source"]),
        destination=_load_adapter(raw["destination"]),
        kiss_icp=_load_kiss_icp(raw.get("kiss_icp", {})),
        poll_timeout_ms=raw.get("poll_timeout_ms", 1000),
    )
