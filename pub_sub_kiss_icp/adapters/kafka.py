"""Apache Kafka adapter using confluent-kafka.

Required extra: ``pip install "pub-sub-kiss-icp[kafka]"``

Configuration reference
-----------------------
.. code-block:: yaml

    source:
      type: kafka
      topic: pointcloud-input
      connection:
        bootstrap.servers: "localhost:9092"
        group.id: kiss-icp-group
        auto.offset.reset: earliest

    destination:
      type: kafka
      topic: odometry-output
      connection:
        bootstrap.servers: "localhost:9092"
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pub_sub_kiss_icp.adapters.base import Publisher, Subscriber


class KafkaSubscriber(Subscriber):
    """Kafka consumer wrapping ``confluent_kafka.Consumer``.

    Args:
        topic: Topic to subscribe to.
        connection: ``confluent_kafka.Consumer`` configuration dict.
    """

    def __init__(self, topic: str, connection: Dict[str, Any]) -> None:
        self._topic = topic
        self._connection = connection
        self._consumer = None
        self._last_msg = None

    def connect(self) -> None:
        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise ImportError(
                "confluent-kafka is required for the Kafka adapter. "
                "Install it with: pip install 'pub-sub-kiss-icp[kafka]'"
            ) from exc
        self._consumer = Consumer(self._connection)
        self._consumer.subscribe([self._topic])

    def receive(self, timeout_ms: int = 1000) -> Optional[bytes]:
        msg = self._consumer.poll(timeout=timeout_ms / 1000.0)
        if msg is None:
            return None
        if msg.error():
            raise RuntimeError(f"Kafka consumer error: {msg.error()}")
        self._last_msg = msg
        return msg.value()

    def acknowledge(self, message: object) -> None:
        # Kafka commit is done per-consumer; call store_offsets if needed.
        # With enable.auto.commit=true (default) this is a no-op.
        if self._consumer and self._last_msg is not None:
            self._consumer.store_offsets(self._last_msg)

    def close(self) -> None:
        if self._consumer:
            self._consumer.close()
            self._consumer = None


class KafkaPublisher(Publisher):
    """Kafka producer wrapping ``confluent_kafka.Producer``.

    Args:
        topic: Topic to publish to.
        connection: ``confluent_kafka.Producer`` configuration dict.
    """

    def __init__(self, topic: str, connection: Dict[str, Any]) -> None:
        self._topic = topic
        self._connection = connection
        self._producer = None

    def connect(self) -> None:
        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise ImportError(
                "confluent-kafka is required for the Kafka adapter. "
                "Install it with: pip install 'pub-sub-kiss-icp[kafka]'"
            ) from exc
        self._producer = Producer(self._connection)

    def publish(self, payload: bytes) -> None:
        self._producer.produce(self._topic, value=payload)
        self._producer.poll(0)

    def close(self) -> None:
        if self._producer:
            self._producer.flush()
            self._producer = None
