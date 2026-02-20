"""In-memory mock adapters used for testing and local development."""

from __future__ import annotations

from collections import deque
from typing import List, Optional

from pub_sub_kiss_icp.adapters.base import Publisher, Subscriber


class MockSubscriber(Subscriber):
    """Subscriber backed by an in-memory queue.

    Messages are pre-loaded via :meth:`push` before the pipeline starts,
    or injected at any time during a test.

    Args:
        topic: Logical topic name (informational only).
        messages: Optional initial list of raw byte payloads to enqueue.
    """

    def __init__(self, topic: str = "mock-input", messages: Optional[list] = None) -> None:
        self.topic = topic
        self._queue: deque = deque(messages or [])
        self._connected = False

    def push(self, payload: bytes) -> None:
        """Enqueue a message payload to be returned by the next :meth:`receive` call."""
        self._queue.append(payload)

    def connect(self) -> None:
        self._connected = True

    def receive(self, timeout_ms: int = 1000) -> Optional[bytes]:
        if not self._connected:
            raise RuntimeError("MockSubscriber.connect() was not called")
        if self._queue:
            return self._queue.popleft()
        return None

    def acknowledge(self, message: object) -> None:
        pass  # No-op for in-memory queue

    def close(self) -> None:
        self._connected = False


class MockPublisher(Publisher):
    """Publisher that stores all published payloads in memory.

    Use :attr:`messages` to inspect what was published.

    Args:
        topic: Logical topic name (informational only).
    """

    def __init__(self, topic: str = "mock-output") -> None:
        self.topic = topic
        self.messages: List[bytes] = []
        self._connected = False

    def connect(self) -> None:
        self._connected = True

    def publish(self, payload: bytes) -> None:
        if not self._connected:
            raise RuntimeError("MockPublisher.connect() was not called")
        self.messages.append(payload)

    def close(self) -> None:
        self._connected = False
