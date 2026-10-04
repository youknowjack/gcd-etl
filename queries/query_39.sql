/*
Name: Languages
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2023-12-16T16:20:33.794Z
*/
SELECT series_language_code
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{snapshot}}
GROUP BY series_language_code
ORDER BY count(1) DESC