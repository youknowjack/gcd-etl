-- Series by Author
-- Data source: GCD DuckDB

SELECT series_name,
         count(distinct(issue_id)) AS issues
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }} AND
        len(filter(story_script, x -> regexp_full_match(x, 'Stan Lee( [\[\(].*)?'))) > 0 AND
        series_language_code = 'en' AND variant_of_issue_id = 0
GROUP BY  series_name
ORDER BY  issues DESC