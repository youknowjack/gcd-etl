/*
Name: 2020-05-01 US/CA Missing on_sale_date since 1995
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2021-11-15T17:18:30.480Z
*/
SELECT series_name, issue_number, publisher_name, publication_date, COUNT(1) as story_count
FROM gcd.gcdissuesnapshot
WHERE snapshot = 20211115 AND on_sale_date=-1 AND publication_date > 19950000 AND issue_number>0 AND series_country_code in ('us','ca')
GROUP BY series_name, issue_number, publisher_name, publication_date
ORDER BY publication_date
