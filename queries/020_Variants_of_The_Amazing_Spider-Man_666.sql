-- Variants of The Amazing Spider-Man 666
-- Data source: GCD DuckDB

SELECT series_name, issue_number_raw, variant_name
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot = {{ snapshot }} AND variant_of_issue_id = 863623
GROUP BY series_name, issue_number_raw, variant_name