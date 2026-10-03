-- Comics By Country
-- Data source: GCD DuckDB

SELECT series_country_code,
         count(distinct(series_id)) AS series,
         count(distinct(issue_id)) AS issues,
         count(distinct(story_id)) AS stories
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }}
GROUP BY  series_country_code
ORDER BY  series DESC