-- Issue Count In 2022 Snapshots
-- Data source: GCD DuckDB

SELECT snapshot,
        count(distinct(issue_id)) as issues
FROM gcd.gcdissuesnapshot
WHERE snapshot > 20220000 AND snapshot < 20230000
GROUP BY snapshot
ORDER BY snapshot DESC