# builder-gcd-etl

ETL pipeline for the [Grand Comics Database](https://www.comics.org/) (GCD) that powers [gcdata.org](https://gcdata.org) — a comic book data analytics platform offering SQL query and dashboard capabilities over GCD snapshots.

Downloads periodic GCD MySQL snapshot dumps, loads them into a local MySQL instance, and exports to Parquet. The Parquet files are queried locally via DuckDB (powering a self-hosted Redash instance) and served publicly via DuckDB WASM at [gcdata.org/explore](https://gcdata.org/explore/).

## Overview

Each GCD snapshot is a MySQL dump of the full database as of a given date. The pipeline joins the core tables (issue, series, publisher, indicia publisher, brand, story, story credits) and writes one denormalized record per story per issue. Output is partitioned by snapshot date so all historical snapshots can be queried together.

The resulting dataset backs [gcdata.org](https://gcdata.org), where:
- A self-hosted **Redash** instance (backed by **DuckDB**) lets users author SQL queries and build dashboards
- A public **[/explore](https://gcdata.org/explore/)** page runs DuckDB WASM in the browser — no account needed

Data and queries are licensed [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/), based on GCD's work.

## Architecture

```
GCD MySQL dump
      │
      ▼
pipeline.py (Python + pyarrow)
      │
      ▼
gcd-parquet/snapshot=YYYYMMDD/*.parquet  (local + S3)
      │
      ├─► DuckDB (local) ──► Redash (Docker)
      │
      └─► DuckDB WASM (browser) ◄── parquet.gcdata.org
```

## Prerequisites

- Python 3.9+, with `pyarrow`, `mysql-connector-python`, `boto3`, `pyyaml`
- MySQL (local instance with a `gcd` user)
- DuckDB CLI (`brew install duckdb`)
- AWS CLI (for S3 uploads)

Install Python dependencies:

```sh
pip install pyarrow mysql-connector-python boto3 pyyaml
```

## Running a snapshot

```sh
./run-all.sh 2026-10-01
```

Steps:
1. Downloads the GCD dump ZIP for the given date (`download.sh`)
2. Loads it into a local `gcd20261001` MySQL database
3. Runs `pipeline.py` to export Parquet
4. Syncs Parquet files to S3 and registers the Athena partition
5. Drops the temporary MySQL database and removes the dump ZIP

Or run just the Parquet export:

```sh
python src/main/python/pipeline.py --snapshot 2026-10-01
```

Output lands in `gcd-parquet/snapshot=20261001/part-0000.parquet` (Snappy-compressed, ~40 MB per snapshot across 4 part files).

## DuckDB setup

Create or refresh the local DuckDB database (a view over all local snapshots):

```sh
./setup-duckdb.sh
```

This creates `gcd-db/gcd.duckdb` with:

```sql
CREATE VIEW gcdissuesnapshot AS
  SELECT * FROM read_parquet('gcd-parquet/snapshot=*/part-*.parquet',
                             hive_partitioning=true, union_by_name=true)
```

The `union_by_name=true` option handles schema evolution across snapshots — columns added in later snapshots come back as `NULL` for older rows.

## Redash

Redash runs in Docker and mounts the DuckDB file. See `compose.yaml` in the `redash/` directory. The custom DuckDB query runner is at `redash/redash/query_runner/duckdb.py`.

Extracted queries (DuckDB dialect) are in `queries/`.

## Athena

`src/main/athena/gcdissuesnapshot.sql` defines the external table over `s3://gcd-parquet/`. After uploading a new snapshot:

```sql
ALTER TABLE gcdissuesnapshot ADD PARTITION (snapshot=20261001)
  LOCATION 's3://gcd-parquet/snapshot=20261001/';
```

## Output schema

Key column groups in the denormalized `gcdissuesnapshot` table:

- **Issue**: `issue_id`, `issue_number`, `publication_date`, `price`, `page_count`, `isbn`, `barcode`, `title`, `rating`, etc.
- **Series**: `series_id`, `series_name`, `series_year_began/ended`, `series_country_code`, `series_language_code`, `series_color`, `series_binding`, etc.
- **Publisher**: `publisher_id`, `publisher_name`, `publisher_country_code`, `publisher_url`
- **Indicia publisher**: `indicia_publisher_id`, `indicia_publisher_name`, year range, surrogate flag
- **Brand**: `brand_id`, `brand_name`, `brand_url`, `brand_names` (array), `brand_count`
- **Story**: `story_id`, `story_title`, `story_feature`, `story_genre`, `story_characters`, `story_type`, creator credit arrays (script, pencils, inks, colors, letters, editing, painting) with optional `_creator_id` arrays
- **Snapshot**: `snapshot` (INTEGER, YYYYMMDD)

> **Schema rule**: new fields must always be appended at the end. Inserting fields in the middle shifts Parquet column positions and breaks Athena readers over existing partitions.

## Project layout

```
src/main/python/     Python ETL and helper scripts
  pipeline.py        Main ETL: MySQL → Parquet
  mysql-load-snapshot.py  MySQL snapshot loader
  update_queries.py  Redash query management
  refresh_query.py   Redash query refresh
src/main/athena/     Athena DDL
queries/             Extracted Redash queries (DuckDB dialect)
setup-duckdb.sh      Create/refresh local DuckDB database
run-all.sh           Full pipeline driver
run-parquet.sh       Single-snapshot Parquet export
sync-parquet.sh      Sync Parquet snapshots locally
upload-athena.sh     S3 sync + Athena partition registration
download.sh          GCD dump download
parquet-robots.txt   robots.txt for parquet.gcdata.org
```

## Historical note

Prior to 2026 the pipeline was a Java/Maven project using the Cloudera CDH 5 distribution to write Parquet via Avro, stored in HDFS, queried via Hive and Presto. It was replaced by a Python/pyarrow pipeline that eliminates the Hadoop, Hive, and Presto daemons. The old Avro schema (`src/main/avro/issue_data.avsc`) and Java source (`src/main/java/`) are preserved in git history.
