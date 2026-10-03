/*
Name: Series Drawn by Curt Swan
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2020-08-19T19:11:25.933Z
*/
SELECT series_name,
         count(distinct(issue_id)) AS issues
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }} AND
        contains(story_pencils, 'Curt Swan') AND
        variant_of_issue_id = 0
GROUP BY  series_name
ORDER BY  issues DESC