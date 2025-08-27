-- Flatten the item_master and item_categories

{{
    config(
        unique_key='item_id',
        incremental_strategy='insert_overwrite',
        options={
            'hoodie.datasource.write.precombine.field': 'item_id',
            'hoodie.datasource.write.operation': 'bulk_insert',
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
