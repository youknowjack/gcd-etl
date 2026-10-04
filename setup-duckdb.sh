#!/bin/bash
# Create or refresh gcd-db/gcd.duckdb with a view over all local Parquet snapshots.
# Run this after adding a new snapshot to gcd-parquet/ or on first setup.
#
# Usage: ./setup-duckdb.sh
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
DB_DIR="$DIR/gcd-db"
DB="$DB_DIR/gcd.duckdb"
PARQUET="$DIR/gcd-parquet"
DOCKER_PARQUET_PATH="/data/gcd-parquet"
REDASH_DIR="$(cd "$DIR/../redash" 2>/dev/null && pwd || echo "")"

mkdir -p "$DB_DIR"

if [ ! -d "$PARQUET" ]; then
  echo "ERROR: $PARQUET not found" >&2
  exit 1
fi

SNAPSHOT_COUNT=$(ls -d "$PARQUET"/snapshot=* 2>/dev/null | wc -l | tr -d ' ')
if [ "$SNAPSHOT_COUNT" -eq 0 ]; then
  echo "ERROR: no snapshot= directories found in $PARQUET" >&2
  exit 1
fi

echo "Found $SNAPSHOT_COUNT snapshots in $PARQUET"

# Create view using host path (DuckDB CLI runs on host)
echo "Building $DB (host path: $PARQUET) ..."
duckdb "$DB" <<SQL
CREATE OR REPLACE VIEW gcdissuesnapshot AS
SELECT *
FROM read_parquet(
    '$PARQUET/snapshot=*/part-*.parquet',
    hive_partitioning = true,
    union_by_name    = true
);
SQL

echo "Done: $DB"

# Update view inside Redash Docker container to use the Docker mount path
UPDATE_PY="
import duckdb
con = duckdb.connect('/data/gcd-db/gcd.duckdb')
con.execute('CREATE OR REPLACE VIEW gcdissuesnapshot AS SELECT * FROM read_parquet(\'$DOCKER_PARQUET_PATH/snapshot=*/part-*.parquet\', hive_partitioning=True, union_by_name=True)')
con.close()
print('Docker view updated: $DOCKER_PARQUET_PATH')
"

if [ -n "$REDASH_DIR" ] && docker compose -f "$REDASH_DIR/compose.yaml" ps -q server 2>/dev/null | grep -q .; then
  echo "Updating view inside Redash container ..."
  docker compose -f "$REDASH_DIR/compose.yaml" exec server python3 -c "$UPDATE_PY"
else
  echo ""
  echo "Redash not running. To update the view inside Docker, run:"
  echo "  docker compose exec server python3 -c \""
  echo "  import duckdb; con = duckdb.connect('/data/gcd-db/gcd.duckdb');"
  echo "  con.execute(\\\"CREATE OR REPLACE VIEW gcdissuesnapshot AS SELECT * FROM read_parquet('$DOCKER_PARQUET_PATH/snapshot=*/part-*.parquet', hive_partitioning=True, union_by_name=True)\\\");"
  echo "  con.close()\""
fi
