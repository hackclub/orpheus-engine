Custom Postgres image for Postgres warehouse.

Image name: `ghcr.io/hackclub/warehouse-postgres:17`

## pgBackRest S3 Backups

The image includes pgBackRest and auto-configures WAL archiving on first init. `PGBACKREST_STANZA` must be set or the container will refuse to start. Set the following environment variables on your container to enable backups to S3-compatible storage:

| Variable | Required | Description |
|---|---|---|
| `PGBACKREST_REPO1_TYPE` | yes | Set to `s3` |
| `PGBACKREST_REPO1_S3_BUCKET` | yes | Bucket name |
| `PGBACKREST_REPO1_S3_REGION` | yes | Bucket region (e.g. `fsn1`) |
| `PGBACKREST_REPO1_S3_ENDPOINT` | yes | S3 endpoint (e.g. `fsn1.your-objectstorage.com` for Hetzner) |
| `PGBACKREST_REPO1_S3_KEY` | yes | S3 access key |
| `PGBACKREST_REPO1_S3_KEY_SECRET` | yes | S3 secret key |
| `PGBACKREST_REPO1_S3_URI_STYLE` | yes* | Set to `path` for non-AWS providers (Hetzner, R2, MinIO, etc.) |
| `PGBACKREST_REPO1_PATH` | yes | Path prefix in the bucket (e.g. `/backups/warehouse`) |
| `PGBACKREST_REPO1_RETENTION_FULL` | no | Number of full backups to retain (default: `2`) |
| `PGBACKREST_STANZA` | yes | Stanza name (use `warehouse`) |
| `PGBACKREST_PG1_PATH` | yes | PostgreSQL data directory (default: `/var/lib/postgresql/data`) |
| `PGBACKREST_LOG_LEVEL_CONSOLE` | no | Log level: `info`, `detail`, `debug` |
| `PGBACKREST_START_FAST` | no | Set to `y` for fast backups |
| `PGBACKREST_PROCESS_MAX` | no | Parallel compression threads (default: `2`) |

### First-time setup

After deploying with the env vars set, exec into the container and run:

```bash
pgbackrest --stanza=warehouse stanza-create
pgbackrest --stanza=warehouse check
```

### Taking backups

```bash
# Full backup
pgbackrest --stanza=warehouse --type=full backup

# Differential backup (requires an existing full)
pgbackrest --stanza=warehouse --type=diff backup
```