import json
from datetime import datetime
from uuid import UUID, uuid4

from confluent_kafka import Consumer
from horizon_data_core.api import initialize_sdk
from horizon_data_core.base_types import DataRow, DataStream, Entity, MetadataRow
from horizon_data_core.client import PostgresClient
from horizon_data_core.helpers import name_to_uuid
from horizon_data_core.sdk import HorizonSDK
from typer import Option, Typer

DEFAULT_REDPANDA_POLL_TIMEOUT: float = -1
DEFAULT_ENTITY_NAME: str = "Unnamed Entity"
DEFAULT_ICEBERG_BATCH_SIZE = 32
DEFAULT_MAX_RETRIES = 10
DEFAULT_POSTGRES_PORT: int = 5432


app = Typer(help="Forwards messages from Redpanda to Horizon")


@app.callback(invoke_without_command=True)
def run(
    redpanda_bootstrap_servers: str = Option(..., help="Redpanda - Bootstrap servers"),
    redpanda_group_id: str = Option(..., help="Redpanda - Group ID"),
    redpanda_topic_names: list[str] = Option(
        ..., "--redpanda-topic-name", help="Redpanda - Topic name"
    ),
    redpanda_poll_timeout: float = Option(
        DEFAULT_REDPANDA_POLL_TIMEOUT, help="Redpanda - Poll timeout"
    ),
    entity_name: str = Option(DEFAULT_ENTITY_NAME, help="Horizon - Entity name"),
    organization_id: UUID | None = Option(None, help="Horizon - Organization ID"),
    iceberg_batch_size: int = Option(
        DEFAULT_ICEBERG_BATCH_SIZE, help="Horizon - Iceberg batch size"
    ),
    max_retries: int = Option(DEFAULT_MAX_RETRIES, help="Horizon - Iceberg batch size"),
    pg_user: str = Option(..., help="Postgres - User"),
    # TODO: Fix ASAP
    pg_password: str = Option(..., help="Postgres - Password"),
    pg_host: str = Option(..., help="Postgres - Host"),
    pg_port: int = Option(DEFAULT_POSTGRES_PORT, help="Postgres - Port"),
    pg_db_name: str = Option(..., help="Postgres - Database name"),
    debug: bool = Option(False, help="Print debug info"),
):
    if organization_id is None:
        organization_id = uuid4()

    redpanda_consumer = setup_redpanda_consumer(
        redpanda_bootstrap_servers, redpanda_group_id, redpanda_topic_names
    )

    pg_client = PostgresClient(
        user=pg_user,
        password=pg_password,
        host=pg_host,
        port=pg_port,
        database=pg_db_name,
    )
    data_stream, sdk = setup_horizon(
        pg_client, entity_name, organization_id, iceberg_batch_size, max_retries
    )

    message_forward_loop(
        redpanda_consumer, redpanda_poll_timeout, data_stream, sdk, debug
    )


def message_forward_loop(
    redpanda_consumer: Consumer,
    redpanda_poll_timeout: float,
    data_stream: DataStream,
    sdk: HorizonSDK,
    debug: bool,
) -> None:
    assert data_stream.id is not None
    while True:
        serialized_message = redpanda_consumer.poll(redpanda_poll_timeout)
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
        if debug:
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
        if debug:
            print(f"Created metadata row: {sdk_metadata_row}")


def setup_horizon(
    pg_client: PostgresClient,
    entity_name: str,
    organization_id: UUID,
    iceberg_batch_size: int,
    max_retries: int,
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
    app()
