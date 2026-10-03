-- Average Price By Year
-- Data source: GCD DuckDB

SELECT CAST(publication_date//10000 as varchar) as year,
        ROUND(avg(cast(regexp_extract(i.price, '[0-9]+\.[0-9]+') as double)), 2) as average_price,
        count(1) as unique_issue_count
FROM gcd.gcdissuesnapshot
CROSS JOIN UNNEST(price) AS i(price)
WHERE snapshot = {{ snapshot }} AND
        series_language_code = '{{ language }}' AND
        series_country_code = '{{ country }}' AND
        page_count = {{ page_count }} AND
        variant_of_issue_id = 0 AND
        regexp_matches(i.price, '^[0-9]+\.[0-9]+ ?{{ currency }}') AND
        publication_date >= {{ start_year }}0000 AND
        publication_date <= {{ end_year }}9999
GROUP BY year
ORDER BY year DESC
LIMIT 100