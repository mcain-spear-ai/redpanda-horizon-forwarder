from datetime import datetime
from uuid import UUID, uuid4

import msgpack
import typer
from confluent_kafka import Consumer, Message
from horizon_data_core.api import initialize_sdk
from horizon_data_core.base_types import DataRow, DataStream, Entity, MetadataRow
from horizon_data_core.client import PostgresClient
from horizon_data_core.helpers import name_to_uuid
from horizon_data_core.sdk import HorizonSDK
from typer import Option, Typer

DEFAULT_REDPANDA_POLL_TIMEOUT: float = -1
DEFAULT_HORIZON_ENTITY_NAME: str = "Unnamed Entity"
DEFAULT_HORIZON_ICEBERG_BATCH_SIZE = 32
DEFAULT_HORIZON_MAX_RETRIES = 10
DEFAULT_HORIZON_POSTGRES_PORT: int = 5432


app = Typer(help="Forwards messages from Redpanda to Horizon")


def main(
    redpanda_bootstrap_servers: str = Option(
        ..., envvar="REDPANDA_BOOTSTRAP_SERVERS", help="Redpanda - Bootstrap servers"
    ),
    redpanda_group_id: str = Option(
        ..., envvar="REDPANDA_GROUP_ID", help="Redpanda - Group ID"
    ),
    redpanda_topic_names: list[str] = Option(
        ...,
        "--redpanda-topic-name",
        envvar="REDPANDA_TOPIC_NAMES",
        help="Redpanda - Topic name",
    ),
    redpanda_poll_timeout: float = Option(
        DEFAULT_REDPANDA_POLL_TIMEOUT,
        envvar="REDPANDA_POLL_TIMEOUT",
        help="Redpanda - Poll timeout",
    ),
    horizon_entity_name: str = Option(
        DEFAULT_HORIZON_ENTITY_NAME,
        envvar="HORIZON_ENTITY_NAME",
        help="Horizon - Entity name",
    ),
    horizon_organization_id: UUID | None = Option(
        None, envvar="HORIZON_ORG_ID", help="Horizon - Organization ID"
    ),
    horizon_iceberg_batch_size: int = Option(
        DEFAULT_HORIZON_ICEBERG_BATCH_SIZE,
        envvar="HORIZON_ICEBERG_BATCH_SIZE",
        help="Horizon - Iceberg batch size",
    ),
    horizon_max_retries: int = Option(
        DEFAULT_HORIZON_MAX_RETRIES,
        envvar="HORIZON_MAX_RETRIES",
        help="Horizon - Max retries",
    ),
    horizon_pg_user: str = Option(
        ..., envvar="PG_USER", help="Horizon - Postgres user"
    ),
    # TODO: Fix ASAP
    horizon_pg_password: str = Option(
        ..., envvar="PG_PASSWORD", help="Horizon - Postgres password"
    ),
    horizon_pg_host: str = Option(
        ..., envvar="PG_HOST", help="Horizon - Postgres host"
    ),
    horizon_pg_port: int = Option(
        DEFAULT_HORIZON_POSTGRES_PORT, envvar="PG_PORT", help="Horizon - Postgres port"
    ),
    horizon_pg_db_name: str = Option(
        ..., envvar="PG_DB_NAME", help="Horizon - Postgres database name"
    ),
    debug: bool = Option(True, envvar="DEBUG", help="Print debug info"),
):
    if horizon_organization_id is None:
        horizon_organization_id = uuid4()

    redpanda_consumer = setup_redpanda_consumer(
        redpanda_bootstrap_servers, redpanda_group_id, redpanda_topic_names
    )

    pg_client = PostgresClient(
        user=horizon_pg_user,
        password=horizon_pg_password,
        host=horizon_pg_host,
        port=horizon_pg_port,
        database=horizon_pg_db_name,
    )
    horizon_data_stream, horizon_sdk = setup_horizon(
        pg_client,
        horizon_entity_name,
        horizon_organization_id,
        horizon_iceberg_batch_size,
        horizon_max_retries,
    )

    message_forward_loop(
        redpanda_consumer,
        redpanda_poll_timeout,
        horizon_data_stream,
        horizon_sdk,
        debug,
    )


def message_forward_loop(
    redpanda_consumer: Consumer,
    redpanda_poll_timeout: float,
    horizon_data_stream: DataStream,
    horizon_sdk: HorizonSDK,
    debug: bool,
) -> None:
    assert horizon_data_stream.id is not None
    while True:
        serialized_redpanda_message = redpanda_consumer.poll(redpanda_poll_timeout)
        if serialized_redpanda_message is None:
            continue

        redpanda_message = deserialize_redpanda_message(serialized_redpanda_message)

        now = datetime.now()

        data_row = DataRow(
            data_stream_id=horizon_data_stream.id,
            datetime=now,
            vector=redpanda_message["vector"],
            data_type=redpanda_message["data_type"],
            track_id=name_to_uuid(redpanda_message["track_name"]),
            vector_start_bound=redpanda_message["vector_start_bound"],
            vector_end_bound=redpanda_message["vector_end_bound"],
        )
        sdk_data_row = horizon_sdk.create_data_row(data_row)
        if debug:
            typer.echo(f"Created data row: {sdk_data_row}")

        metadata_row = MetadataRow(
            data_stream_id=horizon_data_stream.id,
            datetime=now,
            latitude=redpanda_message["latitude"],
            longitude=redpanda_message["longitude"],
            altitude=redpanda_message["altitude"],
            speed=redpanda_message["speed"],
            heading=redpanda_message["heading"],
        )
        sdk_metadata_row = horizon_sdk.create_metadata_row(metadata_row)
        if debug:
            typer.echo(f"Created metadata row: {sdk_metadata_row}")


def deserialize_redpanda_message(message: Message) -> dict:
    message_bytes = message.value()
    deserialized_message = msgpack.unpackb(
        message_bytes, raw=False, strict_map_key=False
    )
    return deserialized_message


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
    typer.run(main)
