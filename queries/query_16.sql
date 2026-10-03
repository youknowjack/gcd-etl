/*
Name: Top Superhero Comics Series
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2020-08-19T19:08:54.226Z
*/
SELECT series_name,
         count(distinct(issue_id)) AS issues,
         count(distinct(series_id)) AS series
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }} AND contains(story_genre,'superhero')
GROUP BY  series_name
ORDER BY  issues DESC LIMIT 25