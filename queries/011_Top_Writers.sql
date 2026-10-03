-- Top Writers
-- Data source: GCD DuckDB

SELECT story.writer,
         count(distinct(issue_id)) AS issues,
         count(distinct(series_id)) AS series,
         count(distinct(publisher_id)) AS publishers
FROM "gcd"."gcdissuesnapshot"
CROSS JOIN UNNEST(story_script) AS story(writer)
WHERE snapshot={{ snapshot }} AND
        story.writer NOT LIKE '%?%' AND story.writer != '' AND
        series_language_code = '{{ language }}' AND variant_of_issue_id = 0
GROUP BY  story.writer
ORDER BY  issues DESC
LIMIT 1000