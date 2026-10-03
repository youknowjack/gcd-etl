/*
Name: Issues/Series By Publisher and Publication Year
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2022-06-05T12:50:42.715Z
*/
SELECT CAST(FLOOR(publication_date/10000) as varchar) as year,
    series_name,
    count(distinct(issue_id)) as issue_count
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot = {{ snapshot }} AND
    publication_date > {{ start_year }}0000 AND
    publication_date < {{ end_year }}0000 AND
    variant_of_issue_id = 0 AND
    regexp_like(series_country_code, '{{ country_code_regexp }}') AND
    series_name <> 'Gwandanaland Comics'
GROUP BY FLOOR(publication_date/10000), series_name HAVING count(distinct(issue_id)) > 12
ORDER BY year DESC, issue_count DESC