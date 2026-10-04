-- Comics with valid prices by country
-- Data source: GCD DuckDB

SELECT series_country_code, count(distinct(issue_id)) count
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }} AND len(filter(price, x -> NOT regexp_matches(x, '[none]|^[\?]?$'))) > 0
GROUP BY series_country_code
ORDER BY count DESC