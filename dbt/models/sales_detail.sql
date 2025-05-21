-- Prepare scans with store location and item category for sales analysis
{{ config(unique_key="scan_id", parition_by=["year_month"]) }}


WITH
    scans as (
        SELECT
            scan_id,
            store_id,
            to_timestamp(scan_datetime, 'yyyy-MM-dd HH:mm:ss') as scan_timestamp,
            year_month,
            item_upc,
            unit_qty,
            unit_price,
            cast((unit_qty * unit_price) as decimal(10, 2)) as net_sale
        FROM apex_bronze.retail_scans
        {% if is_incremental() %}
        WHERE DATE(TO_TIMESTAMP(scan_datetime, 'yyyy-MM-dd HH:mm:ss')) = '{{ var("scan_date") }}'
        {% endif %}
    ),
    final as (
        SELECT
            scans.*,
            items_categories.item_id,
            items_categories.category_code,
            items_categories.category_name,
            stores.city,
            stores.state,
            stores.region
        FROM scans
        inner join
            apex_bronze.retail_stores as stores on scans.store_id = stores.store_id
        inner join
            {{ref('items_categories')}} on (scans.item_upc = items_categories.item_upc)
    )
select * from final
