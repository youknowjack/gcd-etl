-- Prices by Page Count Over Time
-- Data source: GCD DuckDB

SELECT CAST(publication_date//10000 as varchar) as year,
    CAST(page_count as varchar) as pages, 
    ROUND(avg(cast(regexp_extract(i.price, '[0-9]+\.[0-9]+') as double)), 2) as average_price,
    count(1) as issue_count
FROM "gcd"."gcdissuesnapshot"
CROSS JOIN UNNEST(price) AS i(price)
WHERE snapshot = {{ snapshot }} AND
    publication_date > {{ start_year }}0000 AND
    publication_date < {{ end_year }}0000 AND
    regexp_matches(i.price, '^[0-9]+\.[0-9]+ ?{{ currency }}') AND
    variant_of_issue_id = 0 AND
    page_count IN ({{page_counts}})
GROUP BY year, page_count
ORDER BY year DESC, page_count DESC