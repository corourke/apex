-- Standalone top 10 query, also see the other versions using intermediate tables
WITH
  sales_by_month AS (
    SELECT
      DATE_TRUNC ( 'month', CAST( parse_datetime (scans.scan_datetime, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP ) ) AS year_month,
      scans.item_upc,
      retail_stores.state,
      retail_stores.timezone AS region,
      item_master.category_code,
      item_categories.category_name,
      SUM(unit_qty) net_units,
      SUM(unit_qty * unit_price) AS net_sales
    FROM retail_scans_rt AS scans
      INNER JOIN retail_stores_ro AS retail_stores ON scans.store_id = retail_stores.store_id
      INNER JOIN retail_item_master_rt AS item_master ON scans.item_upc = item_master.upc_code
      INNER JOIN retail_item_categories_ro AS item_categories ON item_master.category_code = item_categories.category_code
    GROUP BY 1, 2, 3, 4, 5, 6
  )

  -- Doing this in two parts so that we can create different queries
  -- Better to use silver tables if possible
  -- Top 10 sales categories
SELECT
  category_code,
  category_name,
  SUM(net_units) AS net_units,
  SUM(net_sales) AS net_sales
FROM sales_by_month
GROUP BY category_code, category_name
ORDER BY net_sales DESC
LIMIT 10