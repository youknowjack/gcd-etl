-- Price Trends by Year
-- Data source: GCD DuckDB

SELECT CAST(publication_date//10000 as varchar) as year,
    page_count, 
    ROUND(avg(cast(regexp_extract(i.price, '[0-9]+\.[0-9]+') as double)), 2) as average_price,
    count(1) as issue_count
FROM "gcd"."gcdissuesnapshot"
CROSS JOIN UNNEST(price) AS i(price)
WHERE snapshot = {{ snapshot }} AND
    page_count = {{ page_count }} AND
    publication_date < year(current_date)*10000 AND
    publication_date > 19030000 AND
    regexp_matches(i.price, '^[0-9]+\.[0-9]+ ?{{ currency }}') AND
    variant_of_issue_id = 0
GROUP BY year, page_count
ORDER BY year DESC, issue_count DESC