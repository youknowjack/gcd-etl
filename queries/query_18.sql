/*
Name: Most Common Prices
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2023-12-16T16:51:14.012Z
*/
SELECT i.price,
        count(distinct(issue_id)) as issues
FROM "gcd"."gcdissuesnapshot"
CROSS JOIN UNNEST(price) AS i(price)
WHERE snapshot = {{ snapshot }} AND
        i.price LIKE '%{{ currency }}%' AND
        publication_date >= {{ start_year }}0000 AND
        publication_date <= {{ end_year }}9999
GROUP BY i.price
ORDER BY issues DESC
