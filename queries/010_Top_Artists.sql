-- Top Artists
-- Data source: GCD DuckDB

SELECT story.artist,
        count(distinct(issue_id)) as issues,
        count(distinct(series_id)) as series,
        count(distinct(publisher_id)) as publishers
FROM (
    SELECT issue_id, series_id, publisher_id, story_pencils
    FROM gcd.gcdissuesnapshot
    WHERE snapshot={{ snapshot }} AND variant_of_issue_id=0
)
CROSS JOIN UNNEST(story_pencils) AS story(artist)
WHERE story.artist not like '%?%'
  AND story.artist NOT IN ('', 'various')
GROUP BY story.artist
ORDER BY issues DESC
LIMIT 1000
