-- Issues by ...
-- Data source: GCD DuckDB

SELECT series_name, issue_number
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }} AND
        len(filter(story_script, x -> x = '{{writer}}')) > 0 AND
        series_language_code = 'en' AND variant_of_issue_id = 0
GROUP BY series_name, issue_number
ORDER BY series_name, issue_number