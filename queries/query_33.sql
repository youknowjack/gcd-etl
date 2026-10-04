/*
Name: Prices by Page Count Over Time
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2022-06-01T21:11:13.325Z
*/
SELECT CAST(publication_date//10000 as varchar) as year,
    CAST(page_count as varchar) as pages,
    ROUND(avg(cast(regexp_extract(i.price, '[0-9]+\.[0-9]+') as double)), 2) as average_price,
    count(1) as issue_count
FROM (
    SELECT publication_date, page_count, price
    FROM gcd.gcdissuesnapshot
    WHERE snapshot = {{ snapshot }} AND
        publication_date > {{ start_year }}0000 AND
        publication_date < {{ end_year }}0000 AND
        variant_of_issue_id = 0 AND
        page_count IN ({{page_counts}})
)
CROSS JOIN UNNEST(price) AS i(price)
WHERE regexp_matches(i.price, '^[0-9]+\.[0-9]+ ?{{ currency }}')
GROUP BY year, page_count
ORDER BY year DESC, page_count DESC
