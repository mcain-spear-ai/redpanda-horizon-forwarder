# Redpanda Horizon Forwarder

Forwards messages from Redpanda to Horizon

Note: It looks like Horizon does a lot on import, so startup may take some time

## Run
```sh
$ uv run main.py
```

## Arguments

### Required

 - Redpanda
   - Bootstrap servers: `$REDPANDA_BOOTSTRAP_SERVERS` or `--redpanda-bootstrap-servers`
   - Group ID: `$REDPANDA_GROUP_ID` or `--redpanda-group-id`
   - Topic names: `$REDPANDA_TOPIC_NAMES` or `--redpanda-topic-name` (at least one)
 - Horizon (Postgres)
   - User: `$HORIZON_PG_USER` or `--horizon-pg-user`
   - Password: `$HORIZON_PG_PASSWORD` or `--horizon-pg-password`
   - Host: `$HORIZON_PG_HOST` or `--horizon-pg-host`
   - Database Name: `$HORIZON_PG_DB_NAME` or `--horizon-pg-db-name`

### Env Files

- `.env.forwarder.template` - Example minimal config for the forwarder
- `.env.forwarder` - Actual forwarder config
- `.env.redpanda` - Redpanda config
- `.env.horizon-pg` - Postgres config for Horizon

### CLI Usage
```
 Usage: main.py [OPTIONS]

╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *  --redpanda-bootstrap-servers                  TEXT     Redpanda - Bootstrap servers [env var: REDPANDA_BOOTSTRAP_SERVERS] [required]    │
│ *  --redpanda-group-id                           TEXT     Redpanda - Group ID [env var: REDPANDA_GROUP_ID] [required]                      │
│ *  --redpanda-topic-name                         TEXT     Redpanda - Topic name [env var: REDPANDA_TOPIC_NAMES] [required]                 │
│    --redpanda-poll-timeout                       FLOAT    Redpanda - Poll timeout [env var: REDPANDA_POLL_TIMEOUT] [default: -1]           │
│    --horizon-entity-name                         TEXT     Horizon - Entity name [env var: HORIZON_ENTITY_NAME] [default: Unnamed Entity]   │
│    --horizon-organization-id                     UUID     Horizon - Organization ID [env var: HORIZON_ORG_ID]                              │
│    --horizon-iceberg-batch-size                  INTEGER  Horizon - Iceberg batch size [env var: HORIZON_ICEBERG_BATCH_SIZE] [default: 32] │
│    --horizon-max-retries                         INTEGER  Horizon - Max retries [env var: HORIZON_MAX_RETRIES] [default: 10]               │
│ *  --horizon-pg-user                             TEXT     Horizon - Postgres user [env var: PG_USER] [required]                            │
│ *  --horizon-pg-password                         TEXT     Horizon - Postgres password [env var: PG_PASSWORD] [required]                    │
│ *  --horizon-pg-host                             TEXT     Horizon - Postgres host [env var: PG_HOST] [required]                            │
│    --horizon-pg-port                             INTEGER  Horizon - Postgres port [env var: PG_PORT] [default: 5432]                       │
│ *  --horizon-pg-db-name                          TEXT     Horizon - Postgres database name [env var: PG_DB_NAME] [required]                │
│    --debug                         --no-debug             Print debug info [env var: DEBUG] [default: no-debug]                            │
│    --help                                                 Show this message and exit.                                                      │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```
