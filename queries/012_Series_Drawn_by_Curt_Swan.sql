-- Series Drawn by Curt Swan
-- Data source: GCD DuckDB

SELECT series_name,
         count(distinct(issue_id)) AS issues
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }} AND
        contains(story_pencils, 'Curt Swan') AND
        variant_of_issue_id = 0
GROUP BY  series_name
ORDER BY  issues DESC