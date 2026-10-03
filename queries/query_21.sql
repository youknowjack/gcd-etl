/*
Name: Modified in Oct 2019
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2020-08-19T16:08:38.221Z
*/
SELECT modified,
        count(distinct(series_id)) as series,
        count(distinct(issue_id)) as issues,
        count(distinct(story_id)) as stories
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot = {{ snapshot }} AND modified < 20191101 AND modified > 20191000
GROUP BY modified