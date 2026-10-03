#!/bin/bash
# Sync /gcd/parquet/ from HDFS to ./gcd-parquet/, skipping snapshots already present locally.

set -euo pipefail

LOCAL_DIR="$(cd "$(dirname "$0")/gcd-parquet" && pwd)"
HDFS_DIR="/gcd/parquet"

echo "Local:  $LOCAL_DIR"
echo "Remote: $HDFS_DIR"
echo ""

missing=()
while IFS= read -r line; do
  # Extract the last path component (e.g. "snapshot=20150101")
  name="${line##*/}"
  [[ -z "$name" ]] && continue
  if [[ ! -d "$LOCAL_DIR/$name" ]]; then
    missing+=("$name")
  fi
done < <(hadoop fs -ls "$HDFS_DIR" 2>/dev/null | awk '{print $NF}' | grep '/')

total=${#missing[@]}
if [[ $total -eq 0 ]]; then
  echo "Already in sync — nothing to download."
  exit 0
fi

echo "Snapshots to download: $total"
echo ""

count=0
for name in "${missing[@]}"; do
  count=$((count + 1))
  echo "[$count/$total] $name"
  hadoop fs -get "$HDFS_DIR/$name" "$LOCAL_DIR/$name"
done

echo ""
echo "Done. Downloaded $total snapshot(s)."
