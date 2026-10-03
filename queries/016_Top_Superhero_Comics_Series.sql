-- Top Superhero Comics Series
-- Data source: GCD DuckDB

SELECT series_name,
         count(distinct(issue_id)) AS issues,
         count(distinct(series_id)) AS series
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }} AND contains(story_genre,'superhero')
GROUP BY  series_name
ORDER BY  issues DESC LIMIT 25