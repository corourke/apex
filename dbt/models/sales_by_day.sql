-- Roll up sales by day, category, region

{{
    config(
        materialized='table',
        unique_key='scan_date,category_code,region',
        pre_hook=[ "SET hoodie.simple.index.parallelism=1000" ]
    )
}}

SELECT 
    DATE(scan_timestamp) AS scan_date, 
    category_code, 
    category_name,
    region, 
    sum(unit_qty) net_units,
    sum(net_sale) as net_sales
FROM apex_silver.sales_detail
GROUP BY 1, 2, 3, 4