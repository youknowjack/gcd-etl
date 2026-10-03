/*
Name: Top 5 Publishing Countries in 2019
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2020-08-18T04:58:55.676Z
*/
SELECT series_country_code, count(distinct(series_id)) as "Series Count"
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot=20200115 AND publication_date > 20190000 AND publication_date < 20200000
GROUP BY series_country_code
ORDER BY count(distinct(series_id)) desc
LIMIT 5