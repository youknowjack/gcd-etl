/*
Name: Series Count By Country
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2020-08-18T05:02:42.291Z
*/
SELECT if(series_country_code='us', 'United States', 'Rest of World') as geo, count(distinct(series_id)) as "% of Series"
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }}
GROUP BY series_country_code='us'