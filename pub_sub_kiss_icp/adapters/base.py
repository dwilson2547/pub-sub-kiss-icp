"""Abstract base classes for pub-sub adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional


class Subscriber(ABC):
    """Interface for consuming messages from a pub-sub topic."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection and subscribe to the configured topic."""

    @abstractmethod
    def receive(self, timeout_ms: int = 1000) -> Optional[bytes]:
        """Block until a message arrives or *timeout_ms* elapses.

        Returns:
            Raw message payload bytes, or *None* on timeout.
        """

    @abstractmethod
    def acknowledge(self, message: object) -> None:
        """Acknowledge successful processing of the last received message.

        Some brokers (e.g. Pulsar, Google Pub/Sub) require explicit acks;
        others (e.g. Kafka with auto-commit) may treat this as a no-op.

        Args:
            message: The opaque broker-specific message handle returned
                     alongside the payload by broker-specific receive
                     implementations, or *None* if not applicable.
        """

    @abstractmethod
    def close(self) -> None:
        """Close the connection and release resources."""

    def __enter__(self) -> "Subscriber":
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class Publisher(ABC):
    """Interface for publishing messages to a pub-sub topic."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the broker."""

    @abstractmethod
    def publish(self, payload: bytes) -> None:
        """Publish *payload* to the configured topic."""

    @abstractmethod
    def close(self) -> None:
        """Flush pending messages and close the connection."""

    def __enter__(self) -> "Publisher":
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
