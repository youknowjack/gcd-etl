/*
Name: Top Writers
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2023-12-16T16:29:37.689Z
*/
SELECT story.writer,
         count(distinct(issue_id)) AS issues,
         count(distinct(series_id)) AS series,
         count(distinct(publisher_id)) AS publishers
FROM (
    SELECT issue_id, series_id, publisher_id, story_script
    FROM gcd.gcdissuesnapshot
    WHERE snapshot={{ snapshot }} AND series_language_code = '{{ language }}' AND variant_of_issue_id = 0
)
CROSS JOIN UNNEST(story_script) AS story(writer)
WHERE story.writer NOT LIKE '%?%' AND story.writer != ''
GROUP BY story.writer
ORDER BY issues DESC
LIMIT 1000
