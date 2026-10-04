#!/bin/bash
# Create or refresh gcd-db/gcd.duckdb with a view over all local Parquet snapshots.
# Run this after adding a new snapshot to gcd-parquet/ or on first setup.
#
# The view always uses /data/gcd-parquet (the Docker mount path) so the file
# works correctly when opened by Redash containers. The host parquet directory
# is only used for the snapshot count check.
#
# Usage: ./setup-duckdb.sh
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
DB_DIR="$DIR/gcd-db"
DB="$DB_DIR/gcd.duckdb"
PARQUET="$DIR/gcd-parquet"
DOCKER_PARQUET_PATH="/data/gcd-parquet"

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
echo "Building $DB (view path: $DOCKER_PARQUET_PATH) ..."

duckdb "$DB" <<SQL
CREATE OR REPLACE VIEW gcdissuesnapshot AS
SELECT *
FROM read_parquet(
    '$DOCKER_PARQUET_PATH/snapshot=*/part-*.parquet',
    hive_partitioning = true,
    union_by_name    = true
);
SQL

echo ""
echo "Done: $DB"
echo ""
echo "Next: restart Redash containers to pick up the refreshed view."
