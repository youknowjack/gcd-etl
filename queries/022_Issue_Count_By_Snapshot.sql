-- Issue Count By Snapshot
-- Data source: GCD DuckDB

SELECT snapshot,
        count(distinct(issue_id)) as issues
FROM gcd.gcdissuesnapshot
WHERE snapshot % 10000 IN (101, 401, 701, 1001)
      AND variant_of_issue_id = 0 
      AND series_is_comics_publication = true
GROUP BY snapshot
ORDER BY snapshot DESC