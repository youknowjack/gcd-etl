#!/usr/bin/env python3
"""GCD ETL pipeline: MySQL dump → Parquet.

Usage:
    python pipeline.py --snapshot 2026-10-01 [--steps all|parquet|upload]
    python pipeline.py --snapshot 2026-10-01 --steps parquet   # only export
"""

import argparse
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, date, timezone
from pathlib import Path

import boto3
import mysql.connector
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

ROWS_PER_PART = 2_000_000
BATCH_SIZE = 50_000

# ---------------------------------------------------------------------------
# Parquet schema (matches issue_data.avsc field order exactly)
# ---------------------------------------------------------------------------

SCHEMA = pa.schema([
    pa.field('unix_time',                       pa.int64()),
    pa.field('issue_id',                        pa.int64()),
    pa.field('issue_number_raw',                pa.string()),
    pa.field('issue_number',                    pa.int32()),
    pa.field('publication_date',                pa.int32()),
    pa.field('price_raw',                       pa.string()),
    pa.field('price',                           pa.list_(pa.string())),
    pa.field('page_count',                      pa.int32()),
    pa.field('indicia_frequency',               pa.string()),
    pa.field('isbn',                            pa.string()),
    pa.field('variant_name',                    pa.string()),
    pa.field('variant_of_issue_id',             pa.int64()),
    pa.field('barcode',                         pa.string()),
    pa.field('title',                           pa.string()),
    pa.field('on_sale_date',                    pa.int32()),
    pa.field('rating',                          pa.string()),
    pa.field('volume_not_printed',              pa.bool_()),
    pa.field('editing',                         pa.list_(pa.string())),
    pa.field('notes',                           pa.string()),
    pa.field('created',                         pa.int32()),
    pa.field('modified',                        pa.int32()),
    pa.field('series_id',                       pa.int64()),
    pa.field('series_name',                     pa.string()),
    pa.field('series_year_began',               pa.int32()),
    pa.field('series_year_ended',               pa.int32()),
    pa.field('series_is_current',               pa.bool_()),
    pa.field('series_country_code',             pa.string()),
    pa.field('series_language_code',            pa.string()),
    pa.field('series_has_gallery',              pa.bool_()),
    pa.field('series_is_comics_publication',    pa.bool_()),
    pa.field('series_color',                    pa.string()),
    pa.field('series_dimensions',               pa.string()),
    pa.field('series_paper_stock',              pa.string()),
    pa.field('series_binding',                  pa.list_(pa.string())),
    pa.field('series_publishing_format',        pa.string()),
    pa.field('series_publishing_type',          pa.string()),
    pa.field('series_is_singleton',             pa.bool_()),
    pa.field('series_created',                  pa.int32()),
    pa.field('series_modified',                 pa.int32()),
    pa.field('publisher_id',                    pa.int64()),
    pa.field('publisher_name',                  pa.string()),
    pa.field('publisher_country_code',          pa.string()),
    pa.field('publisher_created',               pa.int32()),
    pa.field('publisher_modified',              pa.int32()),
    pa.field('publisher_url',                   pa.string()),
    pa.field('indicia_publisher_id',            pa.int64()),
    pa.field('indicia_publisher_name',          pa.string()),
    pa.field('indicia_publisher_country_code',  pa.string()),
    pa.field('indicia_publisher_parent_id',     pa.int64()),
    pa.field('indicia_publisher_year_began',    pa.int32()),
    pa.field('indicia_publisher_year_ended',    pa.int32()),
    pa.field('indicia_publisher_is_surrogate',  pa.bool_()),
    pa.field('indicia_publisher_url',           pa.string()),
    pa.field('indicia_publisher_created',       pa.int32()),
    pa.field('indicia_publisher_modified',      pa.int32()),
    pa.field('brand_id',                        pa.int64()),
    pa.field('brand_name',                      pa.string()),
    pa.field('brand_url',                       pa.string()),
    pa.field('brand_created',                   pa.int32()),
    pa.field('brand_modified',                  pa.int32()),
    pa.field('story_id',                        pa.int64()),
    pa.field('story_title',                     pa.string()),
    pa.field('story_feature',                   pa.string()),
    pa.field('story_sequence_number',           pa.int32()),
    pa.field('story_page_count',                pa.int32()),
    pa.field('story_script',                    pa.list_(pa.string())),
    pa.field('story_script_creator_id',         pa.list_(pa.int64())),
    pa.field('story_pencils',                   pa.list_(pa.string())),
    pa.field('story_pencils_creator_id',        pa.list_(pa.int64())),
    pa.field('story_inks',                      pa.list_(pa.string())),
    pa.field('story_inks_creator_id',           pa.list_(pa.int64())),
    pa.field('story_colors',                    pa.list_(pa.string())),
    pa.field('story_colors_creator_id',         pa.list_(pa.int64())),
    pa.field('story_letters',                   pa.list_(pa.string())),
    pa.field('story_letters_creator_id',        pa.list_(pa.int64())),
    pa.field('story_editing',                   pa.list_(pa.string())),
    pa.field('story_editing_creator_id',        pa.list_(pa.int64())),
    pa.field('story_painting',                  pa.list_(pa.string())),
    pa.field('story_painting_creator_id',       pa.list_(pa.int64())),
    pa.field('story_credit_source',             pa.string()),
    pa.field('story_genre',                     pa.list_(pa.string())),
    pa.field('story_characters',                pa.list_(pa.string())),
    pa.field('story_type',                      pa.string()),
    pa.field('story_job_number',                pa.string()),
    pa.field('story_first_line',                pa.string()),
    pa.field('story_created',                   pa.int32()),
    pa.field('story_modified',                  pa.int32()),
    # brand_names / brand_count must remain last (added after story_* to avoid shifting column positions)
    pa.field('brand_names',                     pa.list_(pa.string())),
    pa.field('brand_count',                     pa.int32()),
])

# ---------------------------------------------------------------------------
# Schema flags by year
# Derived from the per-year template YAML files; defaults follow GcdSchema.java.
# ---------------------------------------------------------------------------

def get_schema_flags(year: int) -> dict:
    return {
        'publication_type':   year >= 2017,
        'volume_not_printed': year >= 2018,
        'series_is_singleton': True,
        'story_first_line':   year >= 2018,
        'story_credit':       year >= 2021,
        'multi_brand':        year >= 2026,
    }

# ---------------------------------------------------------------------------
# Credit type fan-out (mirrors GcdStoryCredit.java)
# ---------------------------------------------------------------------------

CREDIT_FIELDS = {
    1: 'story_script',
    2: 'story_pencils',
    3: 'story_inks',
    4: 'story_colors',
    5: 'story_letters',
    6: 'story_editing',
    9: 'story_painting',
}
CREDIT_FANOUT = {
    7:  [2, 3],
    8:  [2, 3, 4],
    10: [1, 2, 3],
    11: [1, 2, 3, 4],
    12: [1, 2, 3, 5],
    13: [1, 2, 3, 4, 5],
}

def _add_credit(credits, story_id, type_id, creator_id, name):
    if type_id in CREDIT_FANOUT:
        for part in CREDIT_FANOUT[type_id]:
            _add_credit(credits, story_id, part, creator_id, name)
    elif type_id in CREDIT_FIELDS:
        field = CREDIT_FIELDS[type_id]
        entry = credits.setdefault(story_id, {}).setdefault(field, {'names': [], 'ids': []})
        entry['names'].append(name)
        entry['ids'].append(creator_id)

def load_story_credits(conn) -> dict:
    """Returns {story_id: {field: {names: [...], ids: [...]}}}"""
    cursor = conn.cursor(dictionary=True, buffered=True)
    cursor.execute("""
        SELECT c.story_id, c.credit_type_id, cr.gcd_official_name AS name, cr.id AS creator_id
        FROM gcd_story_credit c
        INNER JOIN gcd_creator_name_detail n ON c.creator_id = n.id
        INNER JOIN gcd_creator cr ON n.creator_id = cr.id
        ORDER BY c.story_id
    """)
    credits = {}
    for row in cursor:
        _add_credit(credits, row['story_id'], row['credit_type_id'], row['creator_id'], row['name'])
    cursor.close()
    log.info('Loaded story credits for %d stories', len(credits))
    return credits

# ---------------------------------------------------------------------------
# Metadata lookups
# ---------------------------------------------------------------------------

def _load_id_map(conn, table, key_col, val_col) -> dict:
    cur = conn.cursor(dictionary=True, buffered=True)
    cur.execute(f'SELECT {key_col}, {val_col} FROM {table}')
    result = {row[key_col]: row[val_col] for row in cur}
    cur.close()
    return result

def load_metadata(conn, flags: dict) -> dict:
    meta = {
        'country':    _load_id_map(conn, 'stddata_country', 'id', 'code'),
        'language':   _load_id_map(conn, 'stddata_language', 'id', 'code'),
        'story_type': _load_id_map(conn, 'gcd_story_type', 'id', 'name'),
        'pub_type':   _load_id_map(conn, 'gcd_series_publication_type', 'id', 'name')
                      if flags['publication_type'] else {},
    }
    log.info('Loaded metadata: %d countries, %d languages, %d story types, %d pub types',
             len(meta['country']), len(meta['language']),
             len(meta['story_type']), len(meta['pub_type']))
    return meta

# ---------------------------------------------------------------------------
# SQL query builder
# ---------------------------------------------------------------------------

def build_query(flags: dict) -> str:
    cols = [
        'issue.id AS issue_id',
        'issue.number AS issue_number_raw',
        'issue.key_date AS pubdateraw',
        'issue.price',
        'issue.page_count',
        'issue.indicia_frequency',
        'issue.isbn',
        'issue.variant_name',
        'issue.variant_of_id AS variant_of_issue_id',
        'issue.barcode',
        'issue.title',
        'issue.on_sale_date AS onsaledateraw',
        'issue.rating',
    ]
    if flags['volume_not_printed']:
        cols.append('issue.volume_not_printed')
    cols += [
        'issue.editing AS editing',
        'issue.notes AS notes',
        'UNIX_TIMESTAMP(issue.created) AS created',
        'UNIX_TIMESTAMP(issue.modified) AS modified',
        'series.id AS series_id',
        'series.name AS series_name',
        'series.year_began AS series_year_began',
        'series.year_ended AS series_year_ended',
        'series.is_current AS series_is_current',
        'series.country_id AS scountryid',
        'series.language_id AS slangid',
        'series.has_gallery AS series_has_gallery',
        'series.is_comics_publication AS series_is_comics_publication',
        'series.color AS series_color',
        'series.dimensions AS series_dimensions',
        'series.paper_stock AS series_paper_stock',
        'series.binding AS series_binding',
        'series.publishing_format AS series_publishing_format',
    ]
    if flags['publication_type']:
        cols.append('series.publication_type_id AS spubtypeid')
    if flags['series_is_singleton']:
        cols.append('series.is_singleton AS series_is_singleton')
    cols += [
        'UNIX_TIMESTAMP(series.created) AS series_created',
        'UNIX_TIMESTAMP(series.modified) AS series_modified',
        'publisher.id AS publisher_id',
        'publisher.name AS publisher_name',
        'publisher.country_id AS pubcountryid',
        'publisher.url AS publisher_url',
        'UNIX_TIMESTAMP(publisher.created) AS publisher_created',
        'UNIX_TIMESTAMP(publisher.modified) AS publisher_modified',
        'indicia.id AS indicia_publisher_id',
        'indicia.name AS indicia_publisher_name',
        'indicia.country_id AS indpubcountryid',
        'indicia.parent_id AS indicia_publisher_parent_id',
        'indicia.year_began AS indicia_publisher_year_began',
        'indicia.year_ended AS indicia_publisher_year_ended',
        'indicia.is_surrogate AS indicia_publisher_is_surrogate',
        'indicia.url AS indicia_publisher_url',
        'UNIX_TIMESTAMP(indicia.created) AS indicia_publisher_created',
        'UNIX_TIMESTAMP(indicia.modified) AS indicia_publisher_modified',
        'brand.id AS brand_id',
        'brand.name AS brand_name',
        'brand.url AS brand_url',
        'UNIX_TIMESTAMP(brand.created) AS brand_created',
        'UNIX_TIMESTAMP(brand.modified) AS brand_modified',
    ]
    if flags['multi_brand']:
        cols += ['issue_brand.all_brand_names', 'issue_brand.brand_count']
    cols += [
        'story.id AS story_id',
        'story.title AS story_title',
        'story.feature AS story_feature',
        'story.sequence_number AS story_sequence_number',
        'story.page_count AS story_page_count',
        'story.script AS story_script',
        'story.pencils AS story_pencils',
        'story.inks AS story_inks',
        'story.colors AS story_colors',
        'story.letters AS story_letters',
        'story.editing AS story_editing',
        'story.genre AS story_genre',
        'story.characters AS story_characters',
        'story.type_id AS strtypeid',
        'story.job_number AS story_job_number',
    ]
    if flags['story_first_line']:
        cols.append('story.first_line AS story_first_line')
    cols += [
        'UNIX_TIMESTAMP(story.created) AS story_created',
        'UNIX_TIMESTAMP(story.modified) AS story_modified',
    ]

    if flags['multi_brand']:
        brand_join = (
            "LEFT OUTER JOIN ("
            "SELECT ibe.issue_id, MIN(ibe.brand_id) AS brand_id, "
            "GROUP_CONCAT(b.name ORDER BY b.name SEPARATOR ';') AS all_brand_names, "
            "COUNT(*) AS brand_count "
            "FROM gcd_issue_brand_emblem ibe JOIN gcd_brand b ON b.id=ibe.brand_id "
            "GROUP BY ibe.issue_id"
            ") AS issue_brand ON issue_brand.issue_id=issue.id\n"
            "  LEFT OUTER JOIN gcd_brand AS brand ON brand.id=issue_brand.brand_id"
        )
    else:
        brand_join = "LEFT OUTER JOIN gcd_brand AS brand ON issue.brand_id=brand.id"

    return (
        "SELECT\n  " + ",\n  ".join(cols) + "\n"
        "FROM gcd_issue AS issue\n"
        "  INNER JOIN gcd_series AS series ON issue.series_id=series.id\n"
        "  INNER JOIN gcd_publisher AS publisher ON series.publisher_id=publisher.id\n"
        "  LEFT OUTER JOIN gcd_indicia_publisher AS indicia ON issue.indicia_publisher_id=indicia.id\n"
        f"  {brand_join}\n"
        "  LEFT OUTER JOIN gcd_story AS story ON story.issue_id=issue.id"
    )

# ---------------------------------------------------------------------------
# Row mapping helpers
# ---------------------------------------------------------------------------

_DATE_RE = re.compile(r'^(\d{4})-(\d{2})-(\d{2})')

def _parse_date(v) -> int:
    """Convert date string or datetime.date to YYYYMMDD int, -1 on missing/invalid."""
    if v is None or v == '':
        return -1
    if isinstance(v, (date, datetime)):
        return v.year * 10000 + v.month * 100 + v.day
    m = _DATE_RE.match(str(v))
    return int(m.group(1) + m.group(2) + m.group(3)) if m else -1

def _ts_to_date(v) -> int:
    """Convert unix timestamp to YYYYMMDD int (UTC), -1 on null/zero."""
    try:
        ts = int(v)
    except (TypeError, ValueError):
        return -1
    if ts <= 0:
        return -1
    return int(datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y%m%d'))

def _split(v):
    """Split semicolon-delimited string → list, or None."""
    if v is None:
        return None
    parts = [p for p in re.split(r'\s*;\s*', str(v)) if p]
    return parts or None

def _bool(v):
    return bool(v) if v is not None else None

def map_row(row: dict, unix_time: int, flags: dict, meta: dict, story_credits: dict) -> dict:
    r = {}
    r['unix_time']          = unix_time
    r['issue_id']           = row['issue_id']
    r['issue_number_raw']   = row.get('issue_number_raw') or ''
    r['price_raw']          = row.get('price') or ''

    num = row.get('issue_number_raw')
    try:
        r['issue_number'] = int(num) if num else None
    except (ValueError, TypeError):
        r['issue_number'] = None

    r['publication_date']   = _parse_date(row.get('pubdateraw'))
    r['price']              = _split(row.get('price'))
    r['page_count']         = row.get('page_count')
    r['indicia_frequency']  = row.get('indicia_frequency')
    r['isbn']               = row.get('isbn')
    r['variant_name']       = row.get('variant_name')
    r['variant_of_issue_id'] = row.get('variant_of_issue_id')
    r['barcode']            = row.get('barcode')
    r['title']              = row.get('title')
    r['on_sale_date']       = _parse_date(row.get('onsaledateraw')) if row.get('onsaledateraw') is not None else None
    r['rating']             = row.get('rating')
    r['volume_not_printed'] = _bool(row.get('volume_not_printed')) if flags['volume_not_printed'] else None
    r['editing']            = _split(row.get('editing'))
    r['notes']              = row.get('notes')
    r['created']            = _ts_to_date(row.get('created'))
    r['modified']           = _ts_to_date(row.get('modified'))

    r['series_id']                  = row['series_id']
    r['series_name']                = row.get('series_name')
    r['series_year_began']          = row.get('series_year_began')
    r['series_year_ended']          = row.get('series_year_ended')
    r['series_is_current']          = _bool(row.get('series_is_current'))
    r['series_country_code']        = meta['country'].get(row.get('scountryid'))
    r['series_language_code']       = meta['language'].get(row.get('slangid'))
    r['series_has_gallery']         = _bool(row.get('series_has_gallery'))
    r['series_is_comics_publication'] = _bool(row.get('series_is_comics_publication'))
    r['series_color']               = row.get('series_color')
    r['series_dimensions']          = row.get('series_dimensions')
    r['series_paper_stock']         = row.get('series_paper_stock')
    r['series_binding']             = _split(row.get('series_binding'))
    r['series_publishing_format']   = row.get('series_publishing_format')
    r['series_publishing_type']     = meta['pub_type'].get(row.get('spubtypeid')) if flags['publication_type'] else None
    r['series_is_singleton']        = _bool(row.get('series_is_singleton')) if flags['series_is_singleton'] else None
    r['series_created']             = _ts_to_date(row.get('series_created'))
    r['series_modified']            = _ts_to_date(row.get('series_modified'))

    r['publisher_id']           = row.get('publisher_id') or 0
    r['publisher_name']         = row.get('publisher_name')
    r['publisher_country_code'] = meta['country'].get(row.get('pubcountryid'))
    r['publisher_created']      = _ts_to_date(row.get('publisher_created'))
    r['publisher_modified']     = _ts_to_date(row.get('publisher_modified'))
    r['publisher_url']          = row.get('publisher_url')

    r['indicia_publisher_id']           = row.get('indicia_publisher_id')
    r['indicia_publisher_name']         = row.get('indicia_publisher_name')
    r['indicia_publisher_country_code'] = meta['country'].get(row.get('indpubcountryid'))
    r['indicia_publisher_parent_id']    = row.get('indicia_publisher_parent_id')
    r['indicia_publisher_year_began']   = row.get('indicia_publisher_year_began')
    r['indicia_publisher_year_ended']   = row.get('indicia_publisher_year_ended')
    r['indicia_publisher_is_surrogate'] = _bool(row.get('indicia_publisher_is_surrogate'))
    r['indicia_publisher_url']          = row.get('indicia_publisher_url')
    r['indicia_publisher_created']      = _ts_to_date(row.get('indicia_publisher_created'))
    r['indicia_publisher_modified']     = _ts_to_date(row.get('indicia_publisher_modified'))

    r['brand_id']       = row.get('brand_id')
    r['brand_name']     = row.get('brand_name')
    r['brand_url']      = row.get('brand_url')
    r['brand_created']  = _ts_to_date(row.get('brand_created'))
    r['brand_modified'] = _ts_to_date(row.get('brand_modified'))

    story_id = row.get('story_id')
    if story_id is not None:
        r['story_id']              = story_id
        r['story_title']           = row.get('story_title')
        r['story_feature']         = row.get('story_feature')
        r['story_sequence_number'] = row.get('story_sequence_number')
        r['story_page_count']      = row.get('story_page_count')

        credit = story_credits.get(story_id) if flags['story_credit'] else None
        if credit:
            for field in ('story_script', 'story_pencils', 'story_inks', 'story_colors',
                          'story_letters', 'story_editing', 'story_painting'):
                entry = credit.get(field)
                r[field]                    = entry['names'] if entry else None
                r[field + '_creator_id']    = entry['ids']   if entry else None
            r['story_credit_source'] = 'gcd_story_credit'
        else:
            for field in ('story_script', 'story_pencils', 'story_inks', 'story_colors',
                          'story_letters', 'story_editing'):
                r[field]                 = _split(row.get(field))
                r[field + '_creator_id'] = None
            r['story_painting']             = None
            r['story_painting_creator_id']  = None
            r['story_credit_source']        = 'gcd_story'

        r['story_genre']      = _split(row.get('story_genre'))
        r['story_characters'] = _split(row.get('story_characters'))
        r['story_type']       = meta['story_type'].get(row.get('strtypeid'))
        r['story_job_number'] = row.get('story_job_number')
        r['story_first_line'] = row.get('story_first_line') if flags['story_first_line'] else None
        r['story_created']    = _ts_to_date(row.get('story_created'))
        r['story_modified']   = _ts_to_date(row.get('story_modified'))
    else:
        for f in ('story_id', 'story_title', 'story_feature', 'story_sequence_number',
                  'story_page_count', 'story_script', 'story_script_creator_id',
                  'story_pencils', 'story_pencils_creator_id', 'story_inks',
                  'story_inks_creator_id', 'story_colors', 'story_colors_creator_id',
                  'story_letters', 'story_letters_creator_id', 'story_editing',
                  'story_editing_creator_id', 'story_painting', 'story_painting_creator_id',
                  'story_credit_source', 'story_genre', 'story_characters', 'story_type',
                  'story_job_number', 'story_first_line', 'story_created', 'story_modified'):
            r[f] = None

    # brand_names / brand_count: always last (schema ordering constraint)
    r['brand_names'] = _split(row.get('all_brand_names')) if flags['multi_brand'] else None
    r['brand_count'] = row.get('brand_count') if flags['multi_brand'] else None

    return r

# ---------------------------------------------------------------------------
# Parquet export
# ---------------------------------------------------------------------------

def export_parquet(conn, snapshot_date: str, parquet_dir: str, flags: dict,
                   meta: dict, story_credits: dict) -> int:
    compact = snapshot_date.replace('-', '')
    out_dir = Path(parquet_dir) / f'snapshot={compact}'
    out_dir.mkdir(parents=True, exist_ok=True)

    unix_time = int(datetime.strptime(snapshot_date, '%Y-%m-%d')
                    .replace(tzinfo=timezone.utc).timestamp())

    query = build_query(flags)
    log.info('Executing main GCD query...')

    cursor = conn.cursor(dictionary=True, buffered=False)
    cursor.execute(query)

    part = 0
    count = 0
    batch = []

    def _open_writer(p):
        path = out_dir / f'part-{p:04d}.parquet'
        log.info('Writing %s', path)
        return pq.ParquetWriter(str(path), SCHEMA, compression='snappy')

    writer = _open_writer(part)

    def _flush(b):
        tbl = pa.Table.from_pylist(b, schema=SCHEMA)
        writer.write_table(tbl)

    for row in cursor:
        try:
            batch.append(map_row(row, unix_time, flags, meta, story_credits))
        except Exception as e:
            log.warning('Skipping row due to: %s', e)
            continue

        count += 1
        if len(batch) >= BATCH_SIZE:
            _flush(batch)
            batch = []

        if count % 100_000 == 0:
            log.info('Processed %d rows', count)

        if count % ROWS_PER_PART == 0:
            if batch:
                _flush(batch)
                batch = []
            writer.close()
            part += 1
            writer = _open_writer(part)

    if batch:
        _flush(batch)
    writer.close()
    cursor.close()

    log.info('Wrote %d rows to %s (%d part file(s))', count, out_dir, part + 1)
    return count

# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)

def connect_mysql(cfg: dict, compact: str):
    db_cfg = cfg['mysql']
    return mysql.connector.connect(
        host=db_cfg.get('host', 'localhost'),
        port=db_cfg.get('port', 3306),
        user=db_cfg['user'],
        password=db_cfg['password'],
        database=f'gcd{compact}',
        connection_timeout=30,
    )

def step_load_mysql(snapshot_date: str, cfg: dict):
    compact = snapshot_date.replace('-', '')
    pw = cfg['mysql']['password']
    user = cfg['mysql']['user']
    sql_file = f'{snapshot_date}.sql'
    zip_file = f'gcd-dump-{snapshot_date}.zip'

    if not Path(zip_file).exists():
        log.error('Dump not found: %s', zip_file)
        sys.exit(1)

    log.info('Unzipping %s', zip_file)
    subprocess.run(['unzip', '-o', zip_file], check=True)

    log.info('Creating database gcd%s', compact)
    _mysql_exec(user, pw, f'DROP DATABASE IF EXISTS gcd{compact}; CREATE DATABASE gcd{compact};')

    log.info('Loading %s', sql_file)
    with open(sql_file, 'rb') as f:
        subprocess.run(
            ['mysql', f'--user={user}', f'--password={pw}', f'gcd{compact}'],
            stdin=f, check=True
        )

    if compact < '20160901':
        log.info('Loading stddata.sql for old snapshot')
        with open('stddata.sql', 'rb') as f:
            subprocess.run(
                ['mysql', f'--user={user}', f'--password={pw}', f'gcd{compact}'],
                stdin=f, check=True
            )

def step_export(snapshot_date: str, cfg: dict):
    compact = snapshot_date.replace('-', '')
    year = int(snapshot_date[:4])
    flags = get_schema_flags(year)
    log.info('Schema flags for %s: %s', snapshot_date, flags)

    conn = connect_mysql(cfg, compact)
    try:
        meta = load_metadata(conn, flags)
        story_credits = load_story_credits(conn) if flags['story_credit'] else {}
        export_parquet(conn, snapshot_date, cfg.get('parquet_dir', 'gcd-parquet'),
                       flags, meta, story_credits)
    finally:
        conn.close()

def step_upload_s3(snapshot_date: str, cfg: dict):
    compact = snapshot_date.replace('-', '')
    s3_cfg = cfg['s3']
    parquet_bucket = s3_cfg['parquet_bucket']
    archive_bucket = s3_cfg['archive_bucket']
    parquet_dir = cfg.get('parquet_dir', 'gcd-parquet')
    local_snapshot = f'{parquet_dir}/snapshot={compact}'

    # 1. Upload this snapshot to the Athena production bucket
    s3_snap = f's3://{parquet_bucket}/snapshot={compact}/'
    log.info('Uploading %s → %s', local_snapshot, s3_snap)
    subprocess.run(['aws', 's3', 'sync', local_snapshot, s3_snap], check=True)

    # 2. Archive the original dump file(s)
    for dump in Path('.').glob(f'gcd-dump-{snapshot_date}*.zip'):
        log.info('Archiving %s', dump)
        subprocess.run(['aws', 's3', 'cp', str(dump), f's3://{archive_bucket}/'], check=True)

    # 3. Mirror all local parquet to the archive bucket
    log.info('Syncing all parquet → s3://%s/gcd-parquet/', archive_bucket)
    subprocess.run(['aws', 's3', 'sync', parquet_dir,
                    f's3://{archive_bucket}/gcd-parquet/'], check=True)

def step_register_athena(snapshot_date: str, cfg: dict):
    compact = snapshot_date.replace('-', '')
    s3_cfg = cfg['s3']
    parquet_bucket = s3_cfg['parquet_bucket']
    archive_bucket = s3_cfg['archive_bucket']
    region = s3_cfg.get('region', 'us-west-2')
    location = f's3://{parquet_bucket}/snapshot={compact}/'
    ddl = (
        f"ALTER TABLE gcdissuesnapshot "
        f"DROP IF EXISTS PARTITION (snapshot={compact});\n"
        f"ALTER TABLE gcdissuesnapshot "
        f"ADD PARTITION (snapshot={compact}) LOCATION '{location}';"
    )
    athena_cfg = cfg.get('athena', {})
    athena_db = athena_cfg.get('database', 'gcd')
    results_prefix = athena_cfg.get('results_prefix', 'athena-results')
    results_loc = f's3://{archive_bucket}/{results_prefix}/'

    log.info('Registering Athena partition: snapshot=%s', compact)
    client = boto3.client('athena', region_name=region)
    for stmt in ddl.split(';\n'):
        stmt = stmt.strip()
        if not stmt:
            continue
        resp = client.start_query_execution(
            QueryString=stmt,
            QueryExecutionContext={'Database': athena_db},
            ResultConfiguration={'OutputLocation': results_loc},
        )
        log.info('Athena query: %s', resp['QueryExecutionId'])

def step_cleanup(snapshot_date: str, cfg: dict):
    compact = snapshot_date.replace('-', '')
    pw = cfg['mysql']['password']
    user = cfg['mysql']['user']
    log.info('Dropping database gcd%s', compact)
    _mysql_exec(user, pw, f'DROP DATABASE IF EXISTS gcd{compact};')
    for f in [f'{snapshot_date}.sql', f'gcd-dump-{snapshot_date}.zip']:
        if Path(f).exists():
            Path(f).unlink()
            log.info('Deleted %s', f)

def _mysql_exec(user, pw, sql):
    subprocess.run(
        ['mysql', f'--user={user}', f'--password={pw}', '-e', sql],
        check=True
    )

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

STEPS_ALL = ['load_mysql', 'export', 'upload_s3', 'register_athena', 'cleanup']

def main():
    parser = argparse.ArgumentParser(description='GCD ETL pipeline')
    parser.add_argument('--snapshot', required=True,
                        help='Snapshot date YYYY-MM-DD')
    parser.add_argument('--config', default='config.yml',
                        help='Config file (default: config.yml)')
    parser.add_argument('--steps', default='all',
                        help=f'Comma-separated steps or "all". Steps: {", ".join(STEPS_ALL)}')
    args = parser.parse_args()

    if not re.match(r'^\d{4}-\d{2}-\d{2}$', args.snapshot):
        parser.error('--snapshot must be YYYY-MM-DD')

    cfg = load_config(args.config)
    steps = STEPS_ALL if args.steps == 'all' else [s.strip() for s in args.steps.split(',')]

    dispatch = {
        'load_mysql':       lambda: step_load_mysql(args.snapshot, cfg),
        'export':           lambda: step_export(args.snapshot, cfg),
        'upload_s3':        lambda: step_upload_s3(args.snapshot, cfg),
        'register_athena':  lambda: step_register_athena(args.snapshot, cfg),
        'cleanup':          lambda: step_cleanup(args.snapshot, cfg),
    }

    for step in steps:
        if step not in dispatch:
            log.error('Unknown step: %s', step)
            sys.exit(1)
        log.info('=== Step: %s ===', step)
        t0 = __import__('time').time()
        dispatch[step]()
        log.info('=== Step %s done in %.1fs ===', step, __import__('time').time() - t0)

if __name__ == '__main__':
    main()
