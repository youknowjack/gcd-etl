/*
Name: Price Trends by Year
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2022-06-01T19:37:41.860Z
*/
SELECT CAST(FLOOR(publication_date/10000) as varchar) as year,
    page_count, 
    ROUND(avg(cast(regexp_extract(i.price, '[0-9]+\.[0-9]+') as double)), 2) as average_price,
    count(1) as issue_count
FROM "gcd"."gcdissuesnapshot"
CROSS JOIN UNNEST(price) AS i(price)
WHERE snapshot = {{ snapshot }} AND
    page_count = {{ page_count }} AND
    publication_date < extract(YEAR from current_date)*10000 AND
    publication_date > 19030000 AND
    regexp_like(i.price, '^[0-9]+\.[0-9]+ ?{{ currency }}') AND
    variant_of_issue_id = 0
GROUP BY FLOOR(publication_date/10000), page_count
ORDER BY year DESC, issue_count DESC