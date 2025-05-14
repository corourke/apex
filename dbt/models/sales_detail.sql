-- Prepare scans with store location and item category for sales analysis
{{
    config(
        materialized='incremental',
        file_format='hudi',
        incremental_strategy='merge',
        unique_key='scan_id',
        location_root='s3://onehouse-customer-bucket-7a00bf9c/datalake/',
        schema='apex_silver',
        options={
        'type': 'mor',
        'primaryKey': 'scan_id',
        'precombineKey': 'event_timestamp'
        }
    )
}}

WITH stores as (
  SELECT store_id, city, state, region 
  FROM apex_bronze.retail_stores
),

final as (
  SELECT 
    scans.scan_id,
    CAST(parse_datetime(scans.scan_datetime, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP(6)) AS scan_datetime,
    scans.year_month,
    scans.item_id,
    scans.item_upc,
    scans.unit_qty,
    scans.unit_price,
    CAST((unit_qty * unit_price) as decimal(10,2)) AS net_sale,
    items_categories.category_code,
    items_categories.category_name,
    scans.store_id,
    stores.city,
    stores.state,
    stores.region
  FROM apex_bronze.retail_scans AS scans
  INNER JOIN stores on scans.store_id = stores.store_id
  INNER JOIN {{ref('items_categories')}} ON (scans.item_upc = items_categories.item_upc)
) 

SELECT * FROM final