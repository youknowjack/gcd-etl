/*
Name: Stories Using New Credit System, By Snapshot
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2024-02-20T17:57:58.466Z
*/
SELECT snapshot,
        count(distinct(story_id)) as stories
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot >= 20200101 AND story_credit_source='gcd_story_credit' AND
        snapshot % 1000 IN (215, 415, 615, 815, 1015, 1215)
GROUP BY snapshot
ORDER BY snapshot DESC