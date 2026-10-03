/*
Name: Issues by ...
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2020-08-18T05:01:52.511Z
*/
SELECT series_name, issue_number
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }} AND
        cardinality(filter(story_script, x -> x = '{{writer}}')) > 0 AND
        series_language_code = 'en' AND variant_of_issue_id = 0
GROUP BY series_name, issue_number
ORDER BY series_name, issue_number