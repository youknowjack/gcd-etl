-- Most Common Prices
-- Data source: GCD DuckDB

SELECT i.price,
        count(distinct(issue_id)) as issues
FROM "gcd"."gcdissuesnapshot"
CROSS JOIN UNNEST(price) AS i(price)
WHERE snapshot = {{ snapshot }} AND
        i.price LIKE '%{{ currency }}%' AND
        publication_date >= {{ start_year }}0000 AND
        publication_date <= {{ end_year }}9999
GROUP BY i.price
ORDER BY issues DESC
