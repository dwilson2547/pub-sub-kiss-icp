"""Azure Event Hubs adapter.

Required extra: ``pip install "pub-sub-kiss-icp[eventhubs]"``

Event Hubs does not support arbitrary consumer groups via the standard
``EventHubConsumerClient`` – the ``$Default`` consumer group is used unless
overridden.  A Storage Account connection string is required for checkpointing
when using ``EventHubConsumerClient``; supply it via
``connection.checkpoint_store_conn_str`` and ``connection.blob_container_name``.
For simple testing without checkpointing, omit these keys and the adapter will
use an in-memory checkpoint store.

Configuration reference
-----------------------
.. code-block:: yaml

    source:
      type: eventhubs
      topic: pointcloud-input   # Event Hub name
      connection:
        conn_str: "Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
        consumer_group: "$Default"
        # Optional checkpointing (recommended for production):
        # checkpoint_store_conn_str: "DefaultEndpointsProtocol=https;..."
        # blob_container_name: kiss-icp-checkpoints

    destination:
      type: eventhubs
      topic: odometry-output    # Event Hub name
      connection:
        conn_str: "Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pub_sub_kiss_icp.adapters.base import Publisher, Subscriber


class EventHubsSubscriber(Subscriber):
    """Azure Event Hubs consumer using ``azure-eventhub``.

    Args:
        topic: Event Hub name.
        connection: Connection parameters (see module docstring).
    """

    def __init__(self, topic: str, connection: Dict[str, Any]) -> None:
        self._topic = topic
        self._connection = dict(connection)
        self._client = None
        self._buffer: List[bytes] = []
        self._last_event = None

    def connect(self) -> None:
        try:
            from azure.eventhub import EventHubConsumerClient
        except ImportError as exc:
            raise ImportError(
                "azure-eventhub is required for the Event Hubs adapter. "
                "Install it with: pip install 'pub-sub-kiss-icp[eventhubs]'"
            ) from exc

        conn_str = self._connection.pop("conn_str")
        consumer_group = self._connection.pop("consumer_group", "$Default")
        checkpoint_store_conn_str = self._connection.pop("checkpoint_store_conn_str", None)
        blob_container = self._connection.pop("blob_container_name", None)

        checkpoint_store = None
        if checkpoint_store_conn_str and blob_container:
            from azure.eventhub.extensions.checkpointstoreblobaio import (
                BlobCheckpointStore,
            )
            checkpoint_store = BlobCheckpointStore.from_connection_string(
                checkpoint_store_conn_str, blob_container
            )

        self._client = EventHubConsumerClient.from_connection_string(
            conn_str,
            consumer_group=consumer_group,
            eventhub_name=self._topic,
            checkpoint_store=checkpoint_store,
        )

    def receive(self, timeout_ms: int = 1000) -> Optional[bytes]:
        if self._buffer:
            return self._buffer.pop(0)

        received: List[bytes] = []

        def _on_event(partition_context, event):
            if event:
                self._last_event = (partition_context, event)
                received.append(event.body_as_bytes())

        # Use a short receive window to approximate non-blocking polling
        self._client.receive(
            on_event=_on_event,
            max_wait_time=timeout_ms / 1000.0,
        )
        self._buffer.extend(received[1:])
        return received[0] if received else None

    def acknowledge(self, message: object) -> None:
        if self._last_event is not None:
            partition_context, _ = self._last_event
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(partition_context.update_checkpoint())
            except Exception:
                pass  # Best-effort checkpoint update

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None


class EventHubsPublisher(Publisher):
    """Azure Event Hubs producer using ``azure-eventhub``.

    Args:
        topic: Event Hub name.
        connection: Must contain ``conn_str``.
    """

    def __init__(self, topic: str, connection: Dict[str, Any]) -> None:
        self._topic = topic
        self._connection = dict(connection)
        self._client = None

    def connect(self) -> None:
        try:
            from azure.eventhub import EventHubProducerClient
        except ImportError as exc:
            raise ImportError(
                "azure-eventhub is required for the Event Hubs adapter. "
                "Install it with: pip install 'pub-sub-kiss-icp[eventhubs]'"
            ) from exc

        conn_str = self._connection.pop("conn_str")
        self._client = EventHubProducerClient.from_connection_string(
            conn_str,
            eventhub_name=self._topic,
        )

    def publish(self, payload: bytes) -> None:
        from azure.eventhub import EventData

        batch = self._client.create_batch()
        batch.add(EventData(payload))
        self._client.send_batch(batch)

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
