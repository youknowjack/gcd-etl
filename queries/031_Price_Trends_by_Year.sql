-- Price Trends by Year
-- Data source: GCD DuckDB

SELECT CAST(publication_date//10000 as varchar) as year,
    page_count,
    ROUND(avg(cast(regexp_extract(i.price, '[0-9]+\.[0-9]+') as double)), 2) as average_price,
    count(1) as issue_count
FROM (
    SELECT publication_date, page_count, price
    FROM gcd.gcdissuesnapshot
    WHERE snapshot = {{ snapshot }} AND
        page_count = {{ page_count }} AND
        publication_date < year(current_date)*10000 AND
        publication_date > 19030000 AND
        variant_of_issue_id = 0
)
CROSS JOIN UNNEST(price) AS i(price)
WHERE regexp_full_match(i.price, '^[0-9]+\.[0-9]+ ?{{ currency }}')
GROUP BY year, page_count
ORDER BY year DESC, issue_count DESC
