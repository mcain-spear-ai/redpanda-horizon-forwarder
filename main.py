import json
from datetime import datetime
from typing import Any, List, Mapping
from uuid import UUID, uuid4

from confluent_kafka import Consumer, KafkaException, Message
from horizon_data_core.api import initialize_sdk
from horizon_data_core.base_types import DataRow, DataStream, Entity, MetadataRow
from horizon_data_core.client import PostgresClient
from horizon_data_core.helpers import name_to_uuid
from horizon_data_core.sdk import HorizonSDK

POSTGRES_USER: str = "user"
POSTGRES_PASSWORD: str = "password"
POSTGRES_HOST: str = "host"
POSTGRES_PORT: int = 5432
POSTGRES_DB_NAME: str = "database"

REDPANDA_POLL_TIMEOUT: float = -1


def setup_horizon(
    pg_client: PostgresClient, entity_name: str, organization_id
) -> tuple[HorizonSDK, Entity, DataStream]:
    pass


def message_forward_loop(
    consumer: Consumer,
) -> None:

    postgres_client = PostgresClient(
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB_NAME,
    )

    organization_id = uuid4()  # This should come from your user context

    sdk = initialize_sdk(postgres_client, {}, organization_id)

    entity = Entity(
        id=uuid4(),
        name="Example Entity",
        kind_id=uuid4(),
    )
    sdk_entity = sdk.create_entity(entity)

    assert sdk_entity.id is not None
    data_stream = DataStream(
        id=uuid4(),
        entity_id=sdk_entity.id,
    )
    sdk_data_stream = sdk.create_data_stream(data_stream)
    assert sdk_data_stream.id is not None

    while True:
        serialized_message = consumer.poll(REDPANDA_POLL_TIMEOUT)
        if serialized_message is None:
            continue

        message = json.loads(serialized_message.value().decode("utf-8"))

        now = datetime.now()

        data_row = DataRow(
            data_stream_id=sdk_data_stream.id,
            datetime=now,
            vector=message.get("vector"),
            data_type=message.get("data_type"),
            track_id=name_to_uuid(message.get("track_name")),
            vector_start_bound=message.get("vector_start_bound"),
            vector_end_bound=message.get("vector_end_bound"),
        )
        sdk_data_row = sdk.create_data_row(data_row)
        print(f"Created data row: {sdk_data_row}")

        # Create a metadata row in Iceberg table
        metadata_row = MetadataRow(
            data_stream_id=sdk_data_stream.id,
            datetime=now,
            latitude=message.get("latitude"),
            longitude=message.get("longitude"),
            altitude=message.get("altitude"),
            speed=message.get("speed"),
            heading=message.get("heading"),
        )
        sdk_metadata_row = sdk.create_metadata_row(metadata_row)
        print(f"Created metadata row: {sdk_metadata_row}")


def create_redpanda_consumer(
    broker_addresses: str,
    consumer_group: str,
    topic_name: str,
) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": broker_addresses,
            "group.id": consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic_name])
    return consumer


def main():
    pass


if __name__ == "__main__":
    main()
