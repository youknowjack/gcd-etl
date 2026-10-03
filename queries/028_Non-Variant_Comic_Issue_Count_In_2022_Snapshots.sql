-- Non-Variant Comic Issue Count In 2022 Snapshots
-- Data source: GCD DuckDB

SELECT snapshot,
        count(distinct(issue_id)) as issues
FROM gcd.gcdissuesnapshot
WHERE snapshot > 20220000 AND snapshot < 20230000 AND variant_of_issue_id = 0 AND series_is_comics_publication = true
GROUP BY snapshot
ORDER BY snapshot DESC