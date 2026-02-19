"""Apache Pulsar adapter (also covers StreamNative Cloud).

Required extra: ``pip install "pub-sub-kiss-icp[pulsar]"``

StreamNative Cloud differs only in connection parameters – supply the cloud
broker URL and an ``authentication_token`` in the ``connection`` block.

Configuration reference
-----------------------
**Open-source Pulsar:**

.. code-block:: yaml

    source:
      type: pulsar          # or streamnative
      topic: persistent://public/default/pointcloud-input
      connection:
        service_url: "pulsar://localhost:6650"
        subscription_name: kiss-icp-sub

    destination:
      type: pulsar
      topic: persistent://public/default/odometry-output
      connection:
        service_url: "pulsar://localhost:6650"

**StreamNative Cloud:**

.. code-block:: yaml

    source:
      type: streamnative
      topic: persistent://your-org/default/pointcloud-input
      connection:
        service_url: "pulsar+ssl://your-cluster.streamnative.io:6651"
        authentication_token: "<your-api-key>"
        subscription_name: kiss-icp-sub
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pub_sub_kiss_icp.adapters.base import Publisher, Subscriber


class PulsarSubscriber(Subscriber):
    """Pulsar/StreamNative consumer.

    Args:
        topic: Fully qualified Pulsar topic name.
        connection: Client and consumer keyword arguments.  Must contain
                    ``service_url``.  Optionally ``subscription_name``,
                    ``authentication_token``, ``initial_position``, etc.
    """

    def __init__(self, topic: str, connection: Dict[str, Any]) -> None:
        self._topic = topic
        self._connection = dict(connection)
        self._client = None
        self._consumer = None
        self._last_msg = None

    def connect(self) -> None:
        try:
            import pulsar
        except ImportError as exc:
            raise ImportError(
                "pulsar-client is required for the Pulsar/StreamNative adapter. "
                "Install it with: pip install 'pub-sub-kiss-icp[pulsar]'"
            ) from exc

        service_url = self._connection.pop("service_url")
        auth_token = self._connection.pop("authentication_token", None)
        subscription_name = self._connection.pop("subscription_name", "kiss-icp-sub")

        client_kwargs: Dict[str, Any] = {}
        if auth_token:
            client_kwargs["authentication"] = pulsar.AuthenticationToken(auth_token)

        self._client = pulsar.Client(service_url, **client_kwargs)
        self._consumer = self._client.subscribe(
            self._topic,
            subscription_name,
            **self._connection,
        )

    def receive(self, timeout_ms: int = 1000) -> Optional[bytes]:
        try:
            msg = self._consumer.receive(timeout_millis=timeout_ms)
            self._last_msg = msg
            return msg.data()
        except Exception as exc:
            # pulsar.Timeout is raised on timeout; treat all timeouts as None
            if "Timeout" in type(exc).__name__:
                return None
            raise

    def acknowledge(self, message: object) -> None:
        if self._last_msg is not None:
            self._consumer.acknowledge(self._last_msg)

    def close(self) -> None:
        if self._consumer:
            self._consumer.close()
        if self._client:
            self._client.close()
        self._consumer = None
        self._client = None


class PulsarPublisher(Publisher):
    """Pulsar/StreamNative producer.

    Args:
        topic: Fully qualified Pulsar topic name.
        connection: Client and producer keyword arguments.  Must contain
                    ``service_url``.  Optionally ``authentication_token``.
    """

    def __init__(self, topic: str, connection: Dict[str, Any]) -> None:
        self._topic = topic
        self._connection = dict(connection)
        self._client = None
        self._producer = None

    def connect(self) -> None:
        try:
            import pulsar
        except ImportError as exc:
            raise ImportError(
                "pulsar-client is required for the Pulsar/StreamNative adapter. "
                "Install it with: pip install 'pub-sub-kiss-icp[pulsar]'"
            ) from exc

        service_url = self._connection.pop("service_url")
        auth_token = self._connection.pop("authentication_token", None)

        client_kwargs: Dict[str, Any] = {}
        if auth_token:
            client_kwargs["authentication"] = pulsar.AuthenticationToken(auth_token)

        self._client = pulsar.Client(service_url, **client_kwargs)
        self._producer = self._client.create_producer(self._topic, **self._connection)

    def publish(self, payload: bytes) -> None:
        self._producer.send(payload)

    def close(self) -> None:
        if self._producer:
            self._producer.close()
        if self._client:
            self._client.close()
        self._producer = None
        self._client = None
