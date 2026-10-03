/*
Name: Average Price By Year
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2023-12-17T16:13:02.398Z
*/
SELECT CAST(FLOOR(publication_date/10000) as varchar) as year,
        ROUND(avg(cast(regexp_extract(i.price, '[0-9]+\.[0-9]+') as double)), 2) as average_price,
        count(1) as unique_issue_count
FROM gcd.gcdissuesnapshot
CROSS JOIN UNNEST(price) AS i(price)
WHERE snapshot = {{ snapshot }} AND
        series_language_code = '{{ language }}' AND
        series_country_code = '{{ country }}' AND
        page_count = {{ page_count }} AND
        variant_of_issue_id = 0 AND
        regexp_like(i.price, '^[0-9]+\.[0-9]+ ?{{ currency }}') AND
        publication_date >= {{ start_year }}0000 AND
        publication_date <= {{ end_year }}9999
GROUP BY FLOOR(publication_date/10000)
ORDER BY year DESC
LIMIT 100