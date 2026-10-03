-- Stories Using New Credit System, By Snapshot
-- Data source: GCD DuckDB

SELECT snapshot,
        count(distinct(story_id)) as stories
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot >= 20200101 AND story_credit_source='gcd_story_credit' AND
        snapshot % 10000 IN (101, 401, 701, 1001)
GROUP BY snapshot
ORDER BY snapshot DESC