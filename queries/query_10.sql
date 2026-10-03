/*
Name: Top Artists
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2020-08-19T19:12:36.249Z
*/
SELECT story.artist,
        count(distinct(issue_id)) as issues,
        count(distinct(series_id)) as series,
        count(distinct(publisher_id)) as publishers
FROM gcd.gcdissuesnapshot
CROSS JOIN UNNEST(story_pencils) AS story(artist)
WHERE snapshot={{ snapshot }} AND
        story.artist not like '%?%' AND
        story.artist NOT IN ('', 'various') AND
        variant_of_issue_id=0
GROUP BY story.artist
ORDER BY issues DESC
LIMIT 1000
