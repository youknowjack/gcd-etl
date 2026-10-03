/*
Name: Series by Stan Lee
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2020-08-19T19:10:11.009Z
*/
SELECT series_name,
         count(distinct(issue_id)) AS issues
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }} AND
        cardinality(filter(story_script, x -> regexp_like(x, 'Stan Lee( [\[\(].*)?'))) > 0 AND
        series_language_code = 'en' AND variant_of_issue_id = 0
GROUP BY  series_name
ORDER BY  issues DESC