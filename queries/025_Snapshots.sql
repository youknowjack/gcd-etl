-- Snapshots
-- Data source: GCD DuckDB

SELECT snapshot
FROM "gcd"."gcdissuesnapshot"
GROUP BY snapshot
ORDER BY snapshot DESC