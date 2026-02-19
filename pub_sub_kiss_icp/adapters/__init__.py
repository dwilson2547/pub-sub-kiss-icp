"""Adapter registry – resolves a protocol name to Subscriber/Publisher classes."""

from __future__ import annotations

from typing import Tuple, Type

from pub_sub_kiss_icp.adapters.base import Publisher, Subscriber

_REGISTRY: dict = {}


def _lazy_register() -> None:
    global _REGISTRY
    if _REGISTRY:
        return
    from pub_sub_kiss_icp.adapters.kafka import KafkaSubscriber, KafkaPublisher
    from pub_sub_kiss_icp.adapters.pulsar import PulsarSubscriber, PulsarPublisher
    from pub_sub_kiss_icp.adapters.eventhubs import EventHubsSubscriber, EventHubsPublisher
    from pub_sub_kiss_icp.adapters.iggy import IggySubscriber, IggyPublisher
    from pub_sub_kiss_icp.adapters.googlepubsub import (
        GooglePubSubSubscriber,
        GooglePubSubPublisher,
    )
    from pub_sub_kiss_icp.adapters.mock import MockSubscriber, MockPublisher

    _REGISTRY = {
        "kafka": (KafkaSubscriber, KafkaPublisher),
        "pulsar": (PulsarSubscriber, PulsarPublisher),
        # StreamNative uses the same Pulsar client with cloud connection params
        "streamnative": (PulsarSubscriber, PulsarPublisher),
        "eventhubs": (EventHubsSubscriber, EventHubsPublisher),
        "iggy": (IggySubscriber, IggyPublisher),
        "googlepubsub": (GooglePubSubSubscriber, GooglePubSubPublisher),
        "mock": (MockSubscriber, MockPublisher),
    }


def get_subscriber_class(protocol: str) -> Type[Subscriber]:
    """Return the Subscriber class for *protocol*."""
    _lazy_register()
    try:
        return _REGISTRY[protocol.lower()][0]
    except KeyError:
        supported = ", ".join(sorted(_REGISTRY))
        raise ValueError(
            f"Unknown protocol '{protocol}'. Supported: {supported}"
        )


def get_publisher_class(protocol: str) -> Type[Publisher]:
    """Return the Publisher class for *protocol*."""
    _lazy_register()
    try:
        return _REGISTRY[protocol.lower()][1]
    except KeyError:
        supported = ", ".join(sorted(_REGISTRY))
        raise ValueError(
            f"Unknown protocol '{protocol}'. Supported: {supported}"
        )
