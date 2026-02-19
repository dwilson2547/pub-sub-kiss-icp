"""Iggy message streaming adapter.

Required extra: ``pip install "pub-sub-kiss-icp[iggy]"``

Iggy organises messages in Streams → Topics → Partitions.  The ``topic``
field in the config is used as the Iggy *topic name*.  Use the ``connection``
block to specify the stream name, partition ID, and TCP server address.

Configuration reference
-----------------------
.. code-block:: yaml

    source:
      type: iggy
      topic: pointcloud-input        # Iggy topic name
      connection:
        host: "127.0.0.1"
        port: 8090
        stream: kiss-icp             # Iggy stream name
        partition_id: 1
        consumer_group: kiss-icp-cg  # optional; uses polling when omitted
        username: iggy
        password: iggy

    destination:
      type: iggy
      topic: odometry-output
      connection:
        host: "127.0.0.1"
        port: 8090
        stream: kiss-icp
        partition_id: 1
        username: iggy
        password: iggy
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pub_sub_kiss_icp.adapters.base import Publisher, Subscriber


class IggySubscriber(Subscriber):
    """Iggy consumer using the ``iggy-py`` SDK.

    Args:
        topic: Iggy topic name.
        connection: Connection parameters (see module docstring).
    """

    def __init__(self, topic: str, connection: Dict[str, Any]) -> None:
        self._topic = topic
        self._connection = dict(connection)
        self._client = None
        self._offset: int = 0

    def connect(self) -> None:
        try:
            from iggy.client.client import IggyClient
            from iggy.configs.tcp import TcpClientConfig
            from iggy.models.identity import UserIdentity
        except ImportError as exc:
            raise ImportError(
                "iggy-py is required for the Iggy adapter. "
                "Install it with: pip install 'pub-sub-kiss-icp[iggy]'"
            ) from exc

        host = self._connection.get("host", "127.0.0.1")
        port = int(self._connection.get("port", 8090))
        username = self._connection.get("username", "iggy")
        password = self._connection.get("password", "iggy")

        self._stream = self._connection.get("stream", "kiss-icp")
        self._partition_id = int(self._connection.get("partition_id", 1))

        self._client = IggyClient()
        import asyncio

        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            self._client.tcp_connect(
                TcpClientConfig(server_addr=f"{host}:{port}")
            )
        )
        loop.run_until_complete(
            self._client.login_user(username, password)
        )
        loop.close()
        self._loop = asyncio.new_event_loop()

    def receive(self, timeout_ms: int = 1000) -> Optional[bytes]:
        from iggy.models.identifier import Identifier
        from iggy.messages.poll_messages import PollingStrategy

        async def _poll():
            msgs = await self._client.poll_messages(
                stream_id=Identifier(string_value=self._stream),
                topic_id=Identifier(string_value=self._topic),
                partition_id=self._partition_id,
                polling_strategy=PollingStrategy.offset(self._offset),
                count=1,
                auto_commit=True,
            )
            return msgs

        result = self._loop.run_until_complete(_poll())
        if result and len(result) > 0:
            msg = result[0]
            self._offset += 1
            self._last_payload = msg.payload
            return msg.payload
        return None

    def acknowledge(self, message: object) -> None:
        # Iggy uses auto-commit or explicit offset management; polling with
        # auto_commit=True handles acknowledgement implicitly.
        pass

    def close(self) -> None:
        if self._client:
            self._loop.run_until_complete(self._client.logout_user())
            self._loop.close()
            self._client = None


class IggyPublisher(Publisher):
    """Iggy producer using the ``iggy-py`` SDK.

    Args:
        topic: Iggy topic name.
        connection: Connection parameters (see module docstring).
    """

    def __init__(self, topic: str, connection: Dict[str, Any]) -> None:
        self._topic = topic
        self._connection = dict(connection)
        self._client = None

    def connect(self) -> None:
        try:
            from iggy.client.client import IggyClient
            from iggy.configs.tcp import TcpClientConfig
        except ImportError as exc:
            raise ImportError(
                "iggy-py is required for the Iggy adapter. "
                "Install it with: pip install 'pub-sub-kiss-icp[iggy]'"
            ) from exc

        host = self._connection.get("host", "127.0.0.1")
        port = int(self._connection.get("port", 8090))
        username = self._connection.get("username", "iggy")
        password = self._connection.get("password", "iggy")

        self._stream = self._connection.get("stream", "kiss-icp")
        self._partition_id = int(self._connection.get("partition_id", 1))

        self._client = IggyClient()
        import asyncio

        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            self._client.tcp_connect(
                TcpClientConfig(server_addr=f"{host}:{port}")
            )
        )
        loop.run_until_complete(
            self._client.login_user(username, password)
        )
        loop.close()
        self._loop = asyncio.new_event_loop()

    def publish(self, payload: bytes) -> None:
        from iggy.messages.send_messages import Message, Partitioning
        from iggy.models.identifier import Identifier

        async def _send():
            await self._client.send_messages(
                stream_id=Identifier(string_value=self._stream),
                topic_id=Identifier(string_value=self._topic),
                partitioning=Partitioning.partition_id(self._partition_id),
                messages=[Message(payload=payload)],
            )

        self._loop.run_until_complete(_send())

    def close(self) -> None:
        if self._client:
            self._loop.run_until_complete(self._client.logout_user())
            self._loop.close()
            self._client = None
