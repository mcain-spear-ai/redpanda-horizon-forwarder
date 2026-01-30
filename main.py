import json
from datetime import datetime
from uuid import UUID, uuid4

from confluent_kafka import Consumer
from horizon_data_core.api import initialize_sdk
from horizon_data_core.base_types import DataRow, DataStream, Entity, MetadataRow
from horizon_data_core.client import PostgresClient
from horizon_data_core.helpers import name_to_uuid
from horizon_data_core.sdk import HorizonSDK

DEFAULT_POSTGRES_PORT: int = 5432
DEFAULT_REDPANDA_POLL_TIMEOUT: float = -1
DEFAULT_ICEBERG_BATCH_SIZE = 32
DEFAULT_MAX_RETRIES = 10

ENTITY_NAME = "Entity"

POSTGRES_USER: str = "user"
POSTGRES_PASSWORD: str = "password"
POSTGRES_HOST: str = "host"
POSTGRES_PORT: int = DEFAULT_POSTGRES_PORT
POSTGRES_DB_NAME: str = "database"

REDPANDA_BOOTSTRAP_SERVERS: str = ""
REDPANDA_GROUP_ID: str = ""
REDPANDA_TOPIC_NAMES = list[str] = []
REDPANDA_POLL_TIMEOUT: float = DEFAULT_REDPANDA_POLL_TIMEOUT

PRINT_CREATED_ROWS = True


def main():
    redpanda_consumer = setup_redpanda_consumer(
        REDPANDA_BOOTSTRAP_SERVERS, REDPANDA_GROUP_ID, REDPANDA_TOPIC_NAMES
    )

    pg_client = PostgresClient(
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB_NAME,
    )
    data_stream, sdk = setup_horizon(pg_client, ENTITY_NAME)

    message_forward_loop(redpanda_consumer, data_stream, sdk)


def message_forward_loop(
    redpanda_consumer: Consumer, data_stream: DataStream, sdk: HorizonSDK
) -> None:
    assert data_stream.id is not None
    while True:
        serialized_message = redpanda_consumer.poll(REDPANDA_POLL_TIMEOUT)
        if serialized_message is None:
            continue

        # Assuming the Redpanda message is UTF-8 encoded JSON bytes
        message = json.loads(serialized_message.value().decode("utf-8"))

        now = datetime.now()

        data_row = DataRow(
            data_stream_id=data_stream.id,
            datetime=now,
            vector=message.get("vector"),
            data_type=message.get("data_type"),
            track_id=name_to_uuid(message.get("track_name")),
            vector_start_bound=message.get("vector_start_bound"),
            vector_end_bound=message.get("vector_end_bound"),
        )
        sdk_data_row = sdk.create_data_row(data_row)
        if PRINT_CREATED_ROWS:
            print(f"Created data row: {sdk_data_row}")

        metadata_row = MetadataRow(
            data_stream_id=data_stream.id,
            datetime=now,
            latitude=message.get("latitude"),
            longitude=message.get("longitude"),
            altitude=message.get("altitude"),
            speed=message.get("speed"),
            heading=message.get("heading"),
        )
        sdk_metadata_row = sdk.create_metadata_row(metadata_row)
        if PRINT_CREATED_ROWS:
            print(f"Created metadata row: {sdk_metadata_row}")


def setup_horizon(
    pg_client: PostgresClient,
    entity_name: str,
    organization_id: UUID = uuid4(),
    iceberg_batch_size: int = DEFAULT_ICEBERG_BATCH_SIZE,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> tuple[DataStream, HorizonSDK]:
    sdk = initialize_sdk(
        pg_client,
        {},
        organization_id,
        iceberg_batch_size=iceberg_batch_size,
        max_retries=max_retries,
    )

    entity = Entity(
        id=uuid4(),
        name=entity_name,
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

    return (data_stream, sdk)


def setup_redpanda_consumer(
    bootstrap_servers: str,
    group_id: str,
    topic_names: list[str],
) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(topic_names)
    return consumer


if __name__ == "__main__":
    main()
