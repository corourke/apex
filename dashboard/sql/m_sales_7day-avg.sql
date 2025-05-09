-- 7 day moving average of sales
WITH scans AS (
  SELECT
    DATE(parse_datetime(scan_datetime, 'yyyy-MM-dd HH:mm:ss')) AS scan_date,
    CAST((unit_qty * unit_price) AS DECIMAL(10,2)) AS net_sale
  FROM retail_scans_rt
  WHERE unit_qty != 0
),
daily_aggregates AS (
  SELECT 
    scan_date,
    SUM(net_sale) AS total_net_sales,
    COUNT(*) AS trx
  FROM scans
  WHERE scan_date >= DATE(CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles') - INTERVAL '7' DAY
    AND scan_date < DATE(CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles')
  GROUP BY scan_date
)
SELECT 
  ROUND(AVG(total_net_sales), 2) AS avg_net_sales,
  AVG(trx) AS avg_trx
FROM daily_aggregates;