/*
Name: Created in Oct 2019
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2020-08-19T17:29:33.761Z
*/
SELECT created,
        count(distinct(series_id)) as series,
        count(distinct(issue_id)) as issues,
        count(distinct(story_id)) as stories
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot = {{ snapshot }} AND created < 20191101 AND created > 20191000
GROUP BY created