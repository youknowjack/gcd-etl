/*
Name: Unique Issue Count By Publisher and Publication Year
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2022-06-05T12:35:54.656Z
*/
SELECT CAST(FLOOR(publication_date/10000) as varchar) as year,
    publisher_name, 
    count(distinct(issue_id)) as issue_count,
    count(distinct(series_id)) as series_count
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot = {{ snapshot }} AND
    publication_date > {{ start_year }}0000 AND
    publication_date < {{ end_year }}0000 AND
    variant_of_issue_id = 0 AND
    regexp_like(series_country_code, '{{ country_code_regexp }}') AND
    regexp_like(publisher_name, '{{ publisher_regexp }}')
GROUP BY FLOOR(publication_date/10000), publisher_name
ORDER BY year DESC, issue_count DESC