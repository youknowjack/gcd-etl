/*
Name: Most Common Prices
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2023-12-16T16:51:14.012Z
*/
SELECT i.price,
        count(distinct(issue_id)) as issues
FROM (
    SELECT issue_id, price
    FROM gcd.gcdissuesnapshot
    WHERE snapshot = {{ snapshot }} AND
        publication_date >= {{ start_year }}0000 AND
        publication_date <= {{ end_year }}9999
)
CROSS JOIN UNNEST(price) AS i(price)
WHERE i.price LIKE '%{{ currency }}%'
GROUP BY i.price
ORDER BY issues DESC
