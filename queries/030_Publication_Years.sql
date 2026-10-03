-- Publication Years
-- Data source: GCD DuckDB

SELECT CAST(publication_date//10000 as varchar) as year
FROM "gcd"."gcdissuesnapshot"
WHERE snapshot = (extract(YEAR from current_date))*10000+101 AND 
    publication_date > 19000000 AND
    publication_date < (year(current_date)+1)*10000
GROUP BY year
ORDER BY year DESC