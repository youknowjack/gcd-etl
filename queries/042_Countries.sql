-- Countries
-- Data source: GCD DuckDB

SELECT series_country_code
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot=20231215
GROUP BY series_country_code
ORDER BY count(1) DESC