/*
Name: Issues With Most Variants
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2020-08-19T19:07:54.555Z
*/
SELECT variant_of_issue_id,
         series_name,
         issue_number_raw,
         publication_date,
         count(distinct(issue_id)) as issues
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot = {{ snapshot }} AND variant_of_issue_id != 0
GROUP BY variant_of_issue_id,
         series_name,
         issue_number_raw,
         publication_date
ORDER BY issues DESC
LIMIT 100