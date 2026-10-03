-- Prices by Page Count
-- Data source: GCD DuckDB

SELECT CAST(page_count as varchar) as pages, 
    ROUND(avg(cast(regexp_extract(i.price, '[0-9]+\.[0-9]+') as double)), 2) as average_price,
    count(1) as issue_count
FROM "gcd"."gcdissuesnapshot"
CROSS JOIN UNNEST(price) AS i(price)
WHERE snapshot = {{ snapshot }} AND
    publication_date > {{ start_year_inclusive }}*10000 AND
    publication_date < {{ end_year_exclusive }}*10000 AND
    regexp_matches(i.price, '^[0-9]+\.[0-9]+ ?{{ currency }}') AND
    variant_of_issue_id = 0 AND
    page_count > 0
GROUP BY page_count HAVING count(1) > {{ min_issue_count }}
ORDER BY page_count ASC