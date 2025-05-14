-- Flatten the item_master and item_categories

{{
    config(
        materialized='incremental',
        file_format='hudi',
        incremental_strategy='merge',
        unique_key='item_id',
        location_root='s3://onehouse-customer-bucket-7a00bf9c/datalake/',
        schema='apex_silver',
        options={
        'type': 'mor',
        'primaryKey': 'item_id',
        'precombineKey': 'event_timestamp'
        }
    )
}}

WITH items AS (
    SELECT category_code, item_id, upc_code as item_upc, base_price, case_qty 
    FROM apex_bronze.retail_item_master
    WHERE item_id < 50000
),
categories AS (
    SELECT category_code, category_name, category_description 
    FROM apex_bronze.retail_item_categories
),
final AS (
    SELECT  item_id, item_upc, base_price, items.category_code, category_name
    FROM items
    INNER JOIN categories ON (items.category_code = categories.category_code)
)
SELECT * FROM final
