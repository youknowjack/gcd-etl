-- Most Common Page Counts
-- Data source: GCD DuckDB

SELECT page_count,
        count(distinct(issue_id)) as issues
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot={{ snapshot }} AND
        series_language_code = '{{ language }}' AND
        page_count != 0 AND
        publication_date >= {{ start_year }}0000 AND
        publication_date <= {{ end_year}}9999
GROUP BY page_count
ORDER BY issues DESC
LIMIT 1000