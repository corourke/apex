-- Prepare scans with store location and item category for sales analysis

{{
    config(
        unique_key='scan_id',
        parition_by=['year_month']
    )
}}


SELECT 
    scans.scan_id,
    TO_TIMESTAMP(scans.scan_datetime, 'yyyy-MM-dd HH:mm:ss') AS scan_timestamp,
    scans.year_month,
    items_categories.item_id,
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
INNER JOIN apex_bronze.retail_stores AS stores on scans.store_id = stores.store_id
INNER JOIN {{ref('items_categories')}} ON (scans.item_upc = items_categories.item_upc)
{% if is_incremental() %}
WHERE DATE(TO_TIMESTAMP(scans.scan_datetime, 'yyyy-MM-dd HH:mm:ss')) = '{{ var("scan_date") }}'
{% endif %}