-- GCDUpdateSchedule
-- Data source: GCD DuckDB

SELECT CURRENT_DATE = last_day(CURRENT_DATE) OR
      CAST(strftime(CURRENT_DATE, '%Y%m%d') AS INTEGER) % 100 = 15
      AS ready_to_update