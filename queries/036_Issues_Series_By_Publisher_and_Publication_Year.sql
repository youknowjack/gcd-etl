-- Issues/Series By Publisher and Publication Year
-- Data source: GCD DuckDB

SELECT CAST(publication_date//10000 as varchar) as year,
    series_name,
    count(distinct(issue_id)) as issue_count
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot = {{ snapshot }} AND
    publication_date > {{ start_year }}0000 AND
    publication_date < {{ end_year }}0000 AND
    variant_of_issue_id = 0 AND
    regexp_full_match(series_country_code, '{{ country_code_regexp }}') AND
    series_name <> 'Gwandanaland Comics'
GROUP BY year, series_name HAVING count(distinct(issue_id)) > 12
ORDER BY year DESC, issue_count DESC