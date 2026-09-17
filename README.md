# builder-gcd-etl

ETL pipeline for the [Grand Comics Database](https://www.comics.org/) (GCD) that powers [gcdata.org](https://gcdata.org) — a comic book data analytics platform offering SQL query and dashboard capabilities over GCD snapshots.

Downloads periodic GCD MySQL snapshot dumps, loads them into a local MySQL instance, and exports to Parquet (queryable via Presto/Hive/Athena) or Flamdex format (for Imhotep, now inactive).

## Overview

Each GCD snapshot is a MySQL dump of the full database as of a given date. This tool joins the core tables (issue, series, publisher, indicia publisher, brand, story, story credits) and writes one denormalized record per story per issue. The output is partitioned by snapshot date so all historical snapshots can be queried together using Presto or Athena.

The resulting dataset is what backs [gcdata.org](https://gcdata.org), where a self-hosted Redash instance lets users author SQL queries and build dashboards over the data. Data and queries are licensed [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/), based on GCD's work.

## Prerequisites

- Java 8, Maven
- MySQL (local instance with a `gcd` user)
- AWS CLI (for S3 uploads)
- Python 3 + dependencies in `requirements.txt` (optional, for query management scripts)

## Build

```sh
mvn package
mvn dependency:build-classpath -Dmdep.outputFile=classpath.txt
```

## Configuration

Each snapshot needs a YAML config file. Copy from the appropriate year template:

```sh
# Example: config20260915.yml is generated automatically by run-all.sh
sed "s/DATESTAMP/20260915/" 2026-template.yml > config20260915.yml
```

**`example.yml`** shows the required fields:

```yaml
gcdatabase:
  url: jdbc:mysql://localhost/gcdDATESTAMP?serverTimezone=UTC
  user: gcd
  password: yourpassword
  gcdSchema:
    storyCredit: true   # use gcd_story_credit table (vs. legacy gcd_story fields)
    multiBrand: true    # use gcd_issue_brand_emblem many-to-many join
```

### Schema flags

Older GCD snapshots are missing columns added in later schema versions. These flags suppress SQL for columns that don't exist in a given snapshot:

| Flag | Default | Notes |
|---|---|---|
| `publicationType` | true | `series.publication_type_id` |
| `volumeNotPrinted` | true | `issue.volume_not_printed` |
| `seriesIsSingleton` | true | `series.is_singleton` |
| `storyFirstLine` | true | `story.first_line` |
| `storyCredit` | true | Use `gcd_story_credit` table for creator credits |
| `multiBrand` | false | Use `gcd_issue_brand_emblem` many-to-many brand join |

## Running a single snapshot

```sh
# Export to Parquet
./run-parquet.sh 20260915

# Export to Flamdex/Imhotep (uncomment in run-all.sh if needed)
./run-flamdex.sh 20260915
```

`run-parquet.sh` takes a datestamp (YYYYMMDD), converts it to `YYYY-MM-DD`, and calls:

```sh
java -cp ... org.gcd.etl.Main config20260915.yml 2026-09-15 gcd-parquet PARQUET
```

Output lands in `gcd-parquet/snapshot=20260915/part-0000.parquet` (Snappy-compressed).

## Full pipeline: `run-all.sh`

```sh
export mysqlpassword=yourpassword   # or omit to be prompted
./run-all.sh 2026-09-15
```

Steps:
1. Downloads the GCD dump ZIP for the given date (via `mac-download.sh`)
2. Unzips and loads into a local `gcd20260915` MySQL database
3. Generates `config20260915.yml` from the year template if missing
4. Runs Parquet export
5. Uploads Parquet files to HDFS (`upload-hdfs.sh`) and S3 (`aws s3 sync`)
6. Drops the temporary MySQL database and removes the dump ZIP

## Athena / Presto table

`src/main/athena/gcdissuesnapshot.sql` defines the external table over `s3://gcd-parquet/`, partitioned by `snapshot` (int, YYYYMMDD). The same table is exposed through a Presto query engine at [gcdata.org](https://gcdata.org) via Redash. After uploading a new snapshot:

```sql
ALTER TABLE gcdissuesnapshot ADD PARTITION (snapshot=20260915)
  LOCATION 's3://gcd-parquet/snapshot=20260915/';
```

## Output schema

The Avro schema is in `src/main/avro/issue_data.avsc`. Key sections:

- **Issue**: `issue_id`, `issue_number`, `publication_date`, `price`, `page_count`, `isbn`, `barcode`, `title`, `rating`, etc.
- **Series**: `series_id`, `series_name`, `series_year_began/ended`, `series_country_code`, `series_language_code`, `series_color`, `series_binding`, etc.
- **Publisher**: `publisher_id`, `publisher_name`, `publisher_country_code`, `publisher_url`
- **Indicia publisher**: `indicia_publisher_id`, `indicia_publisher_name`, year range, surrogate flag
- **Brand**: `brand_id`, `brand_name`, `brand_url`; plus `brand_names` / `brand_count` when `multiBrand=true`
- **Story**: `story_id`, `story_title`, `story_feature`, `story_genre`, `story_characters`, `story_type`, creator credits (script, pencils, inks, colors, letters, editing, painting) with optional `_creator_id` arrays

> **Schema rule**: new fields must always be appended at the end of `issue_data.avsc`. Inserting fields in the middle shifts Parquet column positions and breaks Athena/Presto readers over existing partitions.

## Project layout

```
src/main/avro/       Avro schema (issue_data.avsc)
src/main/athena/     Athena DDL
src/main/java/       Java ETL source
src/main/python/     Helper scripts (query management, snapshot load)
*-template.yml       Per-year config templates
config*.yml          Per-snapshot configs (generated, not committed)
run-all.sh           Full pipeline driver
run-parquet.sh       Single-snapshot Parquet export
upload-hdfs.sh       HDFS upload
upload-athena.sh     Athena partition registration
```
