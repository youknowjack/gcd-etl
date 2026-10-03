/*
Name: Comics By Country
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2020-08-19T19:09:31.243Z
*/
SELECT series_country_code,
         count(distinct(series_id)) AS series,
         count(distinct(issue_id)) AS issues,
         count(distinct(story_id)) AS stories
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }}
GROUP BY  series_country_code
ORDER BY  series DESC