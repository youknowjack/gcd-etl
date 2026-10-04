-- Currencies
-- Data source: GCD DuckDB

SELECT currency FROM (
    SELECT regexp_extract(i.price, '[A-Z]{3}') as currency
    FROM (
        SELECT price FROM gcd.gcdissuesnapshot WHERE snapshot = {{snapshot}}
    )
    CROSS JOIN UNNEST(price) AS i(price)
    WHERE regexp_matches(i.price, '[0-9]+\.[0-9]+ ?[A-Z]{3}')
)
GROUP BY currency
ORDER BY count(1) DESC
