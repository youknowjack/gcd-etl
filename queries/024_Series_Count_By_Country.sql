-- Series Count By Country
-- Data source: GCD DuckDB

SELECT if(series_country_code='us', 'United States', 'Rest of World') as geo, count(distinct(series_id)) as "% of Series"
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }}
GROUP BY series_country_code='us'