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

mkdir -p "$DB_DIR"

if [ ! -d "$PARQUET" ]; then
  echo "ERROR: $PARQUET not found — run the HDFS extraction first" >&2
  exit 1
fi

SNAPSHOT_COUNT=$(ls -d "$PARQUET"/snapshot=* 2>/dev/null | wc -l | tr -d ' ')
if [ "$SNAPSHOT_COUNT" -eq 0 ]; then
  echo "ERROR: no snapshot= directories found in $PARQUET" >&2
  exit 1
fi

echo "Found $SNAPSHOT_COUNT snapshots in $PARQUET"

# Determine parquet path: use /data/ path when running inside Docker, host path otherwise
if [ -d "/data/gcd-parquet" ]; then
  PARQUET_PATH="/data/gcd-parquet"
else
  PARQUET_PATH="$PARQUET"
fi

echo "Building $DB (parquet path: $PARQUET_PATH) ..."

duckdb "$DB" <<SQL
CREATE OR REPLACE VIEW gcdissuesnapshot AS
SELECT *
FROM read_parquet(
    '$PARQUET_PATH/snapshot=*/part-*.parquet',
    hive_partitioning = true,
    union_by_name    = true
);

SELECT 'View created. Available snapshots (newest 10):' AS status;
SELECT snapshot, COUNT(*) AS rows
FROM gcdissuesnapshot
GROUP BY snapshot
ORDER BY snapshot DESC
LIMIT 10;
SQL

echo ""
echo "Done: $DB"
echo ""
echo "Next: configure Redash DuckDB data source with database path: /data/gcd-db/gcd.duckdb"
