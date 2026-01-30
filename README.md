# Redpanda Horizon Forwarder

Forwards messages from Redpanda to Horizon

Note: It looks like Horizon does a lot on import, so startup may take some time

## Usage
```sh
$ uv run main.py
```

## Arguments

### Required
- `--redpanda-bootstrap-servers`
- `--redpanda-group-id`
- `--redpanda-topic-name` (at least one)
- `--pg-user`
- `--pg-password`
- `--pg-host`
- `--pg-db-name`

### List
```
*  --redpanda-bootstrap-servers                  TEXT     Redpanda - Bootstrap servers [required]
*  --redpanda-group-id                           TEXT     Redpanda - Group ID [required]
*  --redpanda-topic-name                         TEXT     Redpanda - Topic name [required]
   --redpanda-poll-timeout                       FLOAT    Redpanda - Poll timeout [default: -1]
   --entity-name                                 TEXT     Horizon - Entity name [default: Unnamed Entity]
   --organization-id                             UUID     Horizon - Organization ID
   --iceberg-batch-size                          INTEGER  Horizon - Iceberg batch size [default: 32]
   --max-retries                                 INTEGER  Horizon - Iceberg batch size [default: 10]
*  --pg-user                                     TEXT     Postgres - User [required]
*  --pg-password                                 TEXT     Postgres - Password [required]
*  --pg-host                                     TEXT     Postgres - Host [required]
   --pg-port                                     INTEGER  Postgres - Port [default: 5432]
*  --pg-db-name                                  TEXT     Postgres - Database name [required]
   --debug                         --no-debug             Print debug info [default: no-debug]
   --install-completion                                   Install completion for the current shell.
   --show-completion                                      Show completion for the current shell, to copy it or customize the installation.
   --help                                                 Show this message and exit.
```
