"""Google Cloud Pub/Sub adapter.

Required extra: ``pip install "pub-sub-kiss-icp[googlepubsub]"``

Configuration reference
-----------------------
.. code-block:: yaml

    source:
      type: googlepubsub
      topic: pointcloud-input          # Pub/Sub topic *name* (not full path)
      connection:
        project_id: my-gcp-project
        subscription: kiss-icp-sub     # Must already exist in GCP
        # Optional: path to a service-account key JSON file.
        # If omitted, Application Default Credentials (ADC) are used.
        # credentials_file: /path/to/service-account.json

    destination:
      type: googlepubsub
      topic: odometry-output
      connection:
        project_id: my-gcp-project
        # credentials_file: /path/to/service-account.json
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pub_sub_kiss_icp.adapters.base import Publisher, Subscriber


class GooglePubSubSubscriber(Subscriber):
    """Google Cloud Pub/Sub subscriber (synchronous pull).

    Args:
        topic: Pub/Sub topic name (used to construct the subscription path).
        connection: Connection parameters (see module docstring).
    """

    def __init__(self, topic: str, connection: Dict[str, Any]) -> None:
        self._topic = topic
        self._connection = dict(connection)
        self._client = None
        self._subscription_path: str = ""
        self._last_ack_id: Optional[str] = None

    def connect(self) -> None:
        try:
            from google.cloud import pubsub_v1
            from google.oauth2 import service_account
        except ImportError as exc:
            raise ImportError(
                "google-cloud-pubsub is required for the Google Pub/Sub adapter. "
                "Install it with: pip install 'pub-sub-kiss-icp[googlepubsub]'"
            ) from exc

        project_id = self._connection["project_id"]
        subscription = self._connection.get("subscription", f"{self._topic}-sub")
        credentials_file = self._connection.get("credentials_file")

        client_kwargs: Dict[str, Any] = {}
        if credentials_file:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_file
            )
            client_kwargs["credentials"] = credentials

        self._client = pubsub_v1.SubscriberClient(**client_kwargs)
        self._subscription_path = self._client.subscription_path(project_id, subscription)

    def receive(self, timeout_ms: int = 1000) -> Optional[bytes]:
        response = self._client.pull(
            request={
                "subscription": self._subscription_path,
                "max_messages": 1,
            },
            timeout=timeout_ms / 1000.0,
        )
        if not response.received_messages:
            return None
        msg = response.received_messages[0]
        self._last_ack_id = msg.ack_id
        return msg.message.data

    def acknowledge(self, message: object) -> None:
        if self._last_ack_id and self._client:
            self._client.acknowledge(
                request={
                    "subscription": self._subscription_path,
                    "ack_ids": [self._last_ack_id],
                }
            )
            self._last_ack_id = None

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None


class GooglePubSubPublisher(Publisher):
    """Google Cloud Pub/Sub publisher.

    Args:
        topic: Pub/Sub topic name.
        connection: Connection parameters (see module docstring).
    """

    def __init__(self, topic: str, connection: Dict[str, Any]) -> None:
        self._topic = topic
        self._connection = dict(connection)
        self._client = None
        self._topic_path: str = ""

    def connect(self) -> None:
        try:
            from google.cloud import pubsub_v1
            from google.oauth2 import service_account
        except ImportError as exc:
            raise ImportError(
                "google-cloud-pubsub is required for the Google Pub/Sub adapter. "
                "Install it with: pip install 'pub-sub-kiss-icp[googlepubsub]'"
            ) from exc

        project_id = self._connection["project_id"]
        credentials_file = self._connection.get("credentials_file")

        client_kwargs: Dict[str, Any] = {}
        if credentials_file:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_file
            )
            client_kwargs["credentials"] = credentials

        self._client = pubsub_v1.PublisherClient(**client_kwargs)
        self._topic_path = self._client.topic_path(project_id, self._topic)

    def publish(self, payload: bytes) -> None:
        future = self._client.publish(self._topic_path, data=payload)
        future.result()  # Block until the message is published

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
