#!/bin/bash
set -euo pipefail

# Set these for your environment
CONTAINER="${PGBACKREST_CONTAINER:?Set PGBACKREST_CONTAINER to the Coolify container name}"
UPTIME_KUMA_URL="${PGBACKREST_UPTIME_KUMA_URL:?Set PGBACKREST_UPTIME_KUMA_URL to the push monitor URL}"

TYPE="${1:?Usage: pgbackrest-backup.sh <full|diff>}"

echo "Starting pgbackrest ${TYPE} backup..."
docker exec "$CONTAINER" pgbackrest --stanza=warehouse --type="$TYPE" backup
echo "Backup completed successfully."

# Ping Uptime Kuma on success
curl -fsS -m 10 --retry 5 "${UPTIME_KUMA_URL}?status=up&msg=${TYPE}%20backup%20OK&ping="
