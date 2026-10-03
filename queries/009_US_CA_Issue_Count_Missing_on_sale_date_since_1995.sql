-- US/CA Issue Count Missing on_sale_date since 1995
-- Data source: GCD DuckDB

select snapshot, count(distinct(issue_id)) as issue_count
from gcd.gcdissuesnapshot
where series_country_code in ('us', 'ca') and publication_date > 19950000 and on_sale_date is null
group by snapshot
order by snapshot desc