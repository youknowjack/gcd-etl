-- Copy of (#39) Languages
-- Data source: GCD DuckDB

SELECT series_language_code
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{snapshot}}
GROUP BY series_language_code
ORDER BY count(1) DESC