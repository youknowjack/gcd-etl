-- Top 5 Publishing Countries in 2019
-- Data source: GCD DuckDB

SELECT series_country_code, count(distinct(series_id)) as "Series Count"
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{snapshot}} AND publication_date > 20190000 AND publication_date < 20200000
GROUP BY series_country_code
ORDER BY count(distinct(series_id)) desc
LIMIT 5