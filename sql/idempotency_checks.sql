-- Run after executing the workflow twice. Every duplicate_key_count must be 0.

SELECT 'bronze_selic_raw' AS table_name, COUNT(*) AS duplicate_key_count
FROM (
  SELECT series_name, data, source_file, COUNT(*) AS row_count
  FROM desafio_bcb.default.bronze_selic_raw
  GROUP BY series_name, data, source_file
  HAVING COUNT(*) > 1
)
UNION ALL
SELECT 'bronze_ipca_raw' AS table_name, COUNT(*) AS duplicate_key_count
FROM (
  SELECT series_name, data, source_file, COUNT(*) AS row_count
  FROM desafio_bcb.default.bronze_ipca_raw
  GROUP BY series_name, data, source_file
  HAVING COUNT(*) > 1
)
UNION ALL
SELECT 'silver_selic' AS table_name, COUNT(*) AS duplicate_key_count
FROM (
  SELECT series_name, reference_date, COUNT(*) AS row_count
  FROM desafio_bcb.default.silver_selic
  GROUP BY series_name, reference_date
  HAVING COUNT(*) > 1
)
UNION ALL
SELECT 'silver_ipca' AS table_name, COUNT(*) AS duplicate_key_count
FROM (
  SELECT series_name, reference_date, COUNT(*) AS row_count
  FROM desafio_bcb.default.silver_ipca
  GROUP BY series_name, reference_date
  HAVING COUNT(*) > 1
)
UNION ALL
SELECT 'gold_monthly_real_interest' AS table_name, COUNT(*) AS duplicate_key_count
FROM (
  SELECT reference_month, COUNT(*) AS row_count
  FROM desafio_bcb.default.gold_monthly_real_interest
  GROUP BY reference_month
  HAVING COUNT(*) > 1
);

