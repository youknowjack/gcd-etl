/*
Name: Currencies
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2023-12-16T16:44:16.927Z
*/
SELECT currency FROM (
    SELECT regexp_extract(i.price, '[A-Z]{3}') as currency
    FROM (
        SELECT price FROM gcd.gcdissuesnapshot WHERE snapshot = {{snapshot}}
    )
    CROSS JOIN UNNEST(price) AS i(price)
    WHERE regexp_full_match(i.price, '[0-9]+\.[0-9]+ ?[A-Z]{3}')
)
GROUP BY currency
ORDER BY count(1) DESC
