/*
Name: Countries
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2023-12-16T21:30:13.739Z
*/
SELECT series_country_code
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{snapshot}}
GROUP BY series_country_code
ORDER BY count(1) DESC