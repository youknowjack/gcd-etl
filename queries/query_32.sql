/*
Name: Prices by Page Count
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2022-06-04T05:04:53.151Z
*/
SELECT CAST(page_count as varchar) as pages,
    ROUND(avg(cast(regexp_extract(i.price, '[0-9]+\.[0-9]+') as double)), 2) as average_price,
    count(1) as issue_count
FROM (
    SELECT page_count, price
    FROM gcd.gcdissuesnapshot
    WHERE snapshot = {{ snapshot }} AND
        publication_date > {{ start_year_inclusive }}*10000 AND
        publication_date < {{ end_year_exclusive }}*10000 AND
        variant_of_issue_id = 0 AND
        page_count > 0
)
CROSS JOIN UNNEST(price) AS i(price)
WHERE regexp_full_match(i.price, '^[0-9]+\.[0-9]+ ?{{ currency }}')
GROUP BY page_count HAVING count(1) > {{ min_issue_count }}
ORDER BY page_count ASC
