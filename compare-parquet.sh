#!/usr/bin/env bash
# compare-parquet.sh <YYYYMMDD> <config-prefix>
# Builds baseline (main) and optimized (optimize-parquet-export) versions,
# runs both against the same MySQL snapshot, and diffs the Parquet output.
#
# Usage: ./compare-parquet.sh 20250701 config20250701
# The script expects a config file named <config-prefix>.yml in the repo root.

set -euo pipefail

DATE_COMPACT=${1:?Usage: $0 YYYYMMDD config-prefix}
CONFIG_PREFIX=${2:?Usage: $0 YYYYMMDD config-prefix}
DATE_DASHED=$(echo "$DATE_COMPACT" | sed 's/\(....\)\(..\)\(..\)/\1-\2-\3/')

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
CP="$(cat "$REPO_ROOT/classpath.txt")"

BASELINE_CLASSES="$REPO_ROOT/target/classes-baseline"
OPTIMIZED_CLASSES="$REPO_ROOT/target/classes-optimized"
BASELINE_OUT="$REPO_ROOT/target/compare-baseline"
OPTIMIZED_OUT="$REPO_ROOT/target/compare-optimized"

#echo "=== Building baseline (main) ==="
#git stash --quiet
#mvn -q compile -f "$REPO_ROOT/pom.xml"
#rm -rf "$BASELINE_CLASSES"
#cp -r "$REPO_ROOT/target/classes" "$BASELINE_CLASSES"
#git stash pop --quiet

#echo "=== Building optimized (optimize-parquet-export) ==="
#mvn -q compile -f "$REPO_ROOT/pom.xml"
#rm -rf "$OPTIMIZED_CLASSES"
#cp -r "$REPO_ROOT/target/classes" "$OPTIMIZED_CLASSES"

#echo ""
#echo "=== Running BASELINE ==="
#rm -rf "$BASELINE_OUT" && mkdir -p "$BASELINE_OUT"
#TIME_BASELINE_START=$SECONDS
#java -Xmx11G -cp "$BASELINE_CLASSES:$CP" org.gcd.etl.Main \
#    "$REPO_ROOT/${CONFIG_PREFIX}.yml" "$DATE_DASHED" "$BASELINE_OUT" PARQUET
#TIME_BASELINE=$((SECONDS - TIME_BASELINE_START))
#echo "Baseline finished in ${TIME_BASELINE}s"

echo ""
echo "=== Running OPTIMIZED ==="
rm -rf "$OPTIMIZED_OUT" && mkdir -p "$OPTIMIZED_OUT"
TIME_OPT_START=$SECONDS
java -Xmx9G -cp "$OPTIMIZED_CLASSES:$CP" org.gcd.etl.Main \
    "$REPO_ROOT/${CONFIG_PREFIX}.yml" "$DATE_DASHED" "$OPTIMIZED_OUT" PARQUET
TIME_OPT=$((SECONDS - TIME_OPT_START))
echo "Optimized finished in ${TIME_OPT}s"

#echo ""
#echo "=== Converting to JSON for diff ==="
#VENV_PYTHON="$REPO_ROOT/venv/bin/python3"
#PYTHON="${VENV_PYTHON:-python3}"

#$PYTHON -c "import pandas" 2>/dev/null || $PYTHON -m pip install -q pandas
#$PYTHON -c "import pyarrow" 2>/dev/null || $PYTHON -m pip install -q pyarrow

#$PYTHON "$REPO_ROOT/src/main/python/parquet-to-json.py" "$BASELINE_OUT"  "$REPO_ROOT/target/compare-baseline-json"
#$PYTHON "$REPO_ROOT/src/main/python/parquet-to-json.py" "$OPTIMIZED_OUT" "$REPO_ROOT/target/compare-optimized-json"

#echo ""
#echo "=== Sorting and diffing ==="
#BASELINE_SORTED="$REPO_ROOT/target/baseline-all-sorted.json"
#OPTIMIZED_SORTED="$REPO_ROOT/target/optimized-all-sorted.json"

#cat "$REPO_ROOT"/target/compare-baseline-json.*.json  | sort > "$BASELINE_SORTED"
#cat "$REPO_ROOT"/target/compare-optimized-json.*.json | sort > "$OPTIMIZED_SORTED"

#BASELINE_ROWS=$(wc -l < "$BASELINE_SORTED")
#OPTIMIZED_ROWS=$(wc -l < "$OPTIMIZED_SORTED")

#echo ""
#echo "=== Results ==="
#echo "Baseline rows:  $BASELINE_ROWS"
#echo "Optimized rows: $OPTIMIZED_ROWS"
#if diff -q "$BASELINE_SORTED" "$OPTIMIZED_SORTED" > /dev/null 2>&1; then
#    echo "OUTPUT: identical"
#else
#    echo "OUTPUT: DIFFERENCES FOUND (first 50 diff lines):"
#    diff "$BASELINE_SORTED" "$OPTIMIZED_SORTED" | head -50
#fi
