-- Created in Oct 2019
-- Data source: GCD DuckDB

SELECT created,
        count(distinct(series_id)) as series,
        count(distinct(issue_id)) as issues,
        count(distinct(story_id)) as stories
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot = {{ snapshot }} AND created < 20191101 AND created > 20191000
GROUP BY created