/*
Name: Variants of The Amazing Spider-Man 666
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2020-08-19T17:28:49.818Z
*/
SELECT series_name, issue_number_raw, variant_name
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot = {{ snapshot }} AND variant_of_issue_id = 863623
GROUP BY series_name, issue_number_raw, variant_name