-- Sales for today, all stores
WITH scans AS (
 SELECT
    DATE(parse_datetime(scan_datetime, 'yyyy-MM-dd HH:mm:ss')) as scan_date,
    CAST((unit_qty * unit_price) as decimal(10,2)) AS net_sale
  FROM retail_scans_rt
  WHERE unit_qty != 0
  )
  SELECT 
    SUM(net_sale) AS net_sales,
    COUNT(*) AS trx
  FROM scans
  WHERE scan_date >= DATE(CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles')
