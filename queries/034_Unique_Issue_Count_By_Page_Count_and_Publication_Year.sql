-- Unique Issue Count By Page Count and Publication Year
-- Data source: GCD DuckDB

SELECT CAST(publication_date//10000 as varchar) as year,
    CAST(page_count as varchar) as pages, 
    count(distinct(issue_id)) as issue_count
FROM "gcd"."gcdissuesnapshot"
CROSS JOIN UNNEST(price) AS i(price)
WHERE snapshot = {{ snapshot }} AND
    publication_date > {{ start_year }}0000 AND
    publication_date < {{ end_year }}0000 AND
    variant_of_issue_id = 0 AND
    regexp_matches(series_country_code, '{{ country_code_regexp }}') AND
    regexp_matches(i.price, '{{ currency_regexp }}') AND
    page_count IN ({{ page_counts }})
GROUP BY year, page_count
ORDER BY year DESC, page_count DESC