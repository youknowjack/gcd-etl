/*
Name: Publication Years
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2022-06-01T19:13:45.066Z
*/
SELECT CAST(FLOOR(publication_date/10000) as varchar) as year
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot = (extract(YEAR from current_date))*10000+101 AND 
    publication_date > 19000000 AND
    publication_date < (extract(YEAR from current_date)+1)*10000
GROUP BY FLOOR(publication_date/10000)
ORDER BY year DESC