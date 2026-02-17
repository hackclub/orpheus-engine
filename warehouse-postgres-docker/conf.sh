#!/bin/bash
set -e

if [ -z "$PGBACKREST_STANZA" ]; then
  echo "FATAL: PGBACKREST_STANZA is not set. Cannot configure WAL archiving." >&2
  exit 1
fi

cat >> "$PGDATA/postgresql.conf" <<EOF

# WAL archiving for pgBackRest
wal_level = replica
archive_mode = on
archive_command = 'pgbackrest --stanza=$PGBACKREST_STANZA archive-push %p'
archive_timeout = 60
EOF
