-- Issues Missing on_sale_date since 1995
-- Data source: GCD DuckDB

select snapshot, series_id, series_name, series_country_code, issue_number, publication_date
from gcd.gcdissuesnapshot
where snapshot = {{snapshot}} and series_country_code = '{{ country_code }}' and publication_date > {{start_year}}0000 and publication_date < {{end_year_exclusive}}0000 and on_sale_date is null
order by series_name, series_country_code
