-- Flatten the item_master and item_categories
WITH items AS (
    SELECT category_code, item_id, upc_code as item_upc, base_price, case_qty 
    FROM retail_item_master_ro
    WHERE item_id < 50000
),
categories AS (
    SELECT category_code, category_name, category_description 
    FROM retail_item_categories_ro
),
final AS (
    SELECT  item_id, item_upc, base_price, items.category_code, category_name
    FROM items
    INNER JOIN categories ON (items.category_code = categories.category_code)
)
SELECT * FROM final
