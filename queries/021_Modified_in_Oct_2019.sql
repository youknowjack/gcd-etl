-- Modified in Oct 2019
-- Data source: GCD DuckDB

SELECT modified,
        count(distinct(series_id)) as series,
        count(distinct(issue_id)) as issues,
        count(distinct(story_id)) as stories
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot = {{ snapshot }} AND modified < 20191101 AND modified > 20191000
GROUP BY modified