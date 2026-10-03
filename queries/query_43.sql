/*
Name: Copy of (#39) Languages
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2023-12-16T21:21:30.191Z
*/
SELECT series_language_code
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot=20231215
GROUP BY series_language_code
ORDER BY count(1) DESC