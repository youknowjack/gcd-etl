/*
Name: Comics with valid prices by country
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2023-12-16T19:04:29.158Z
*/
SELECT series_country_code, count(distinct(issue_id)) count
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }} AND cardinality(filter(price, x -> NOT regexp_like(x, '[none]|^[\?]?$'))) > 0
GROUP BY series_country_code
ORDER BY count DESC