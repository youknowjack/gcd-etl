-- Unique Comic Issue Count Reaches 2 Million in 2023
-- Data source: GCD DuckDB

SELECT snapshot,
        count(distinct(issue_id)) as issues
FROM gcd.gcdissuesnapshot
WHERE snapshot > 20230300 AND snapshot < 20230900
      AND variant_of_issue_id = 0 
      AND series_is_comics_publication = true
GROUP BY snapshot
ORDER BY snapshot DESC