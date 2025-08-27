-- Prepare scans with store location and item category for sales analysis
{{
    config(
        unique_key="scan_id",
        partition_by=["scan_datestamp"],
        options={
            'hoodie.datasource.write.partitionpath.field': 'scan_datestamp'
        }
    )
}}

--{% set scan_date = var("scan_date", (modules.datetime.datetime.now() - modules.datetime.timedelta(days=1)).strftime('%Y-%m-%d')) %}
-- Jinja vs. Python expression so that it evaluates in the job runner
{% set scan_date = var('scan_date', (run_started_at - modules.datetime.timedelta(days=1)).strftime('%Y-%m-%d')) %}

WITH
    scans AS (
        SELECT
            scan_id,
            store_id,
            to_timestamp(scan_datetime, 'yyyy-MM-dd HH:mm:ss') AS scan_timestamp,
            to_date(to_timestamp(scan_datetime, 'yyyy-MM-dd HH:mm:ss')) as scan_datestamp,
            item_upc,
            unit_qty,
            unit_price,
            CAST((unit_qty * unit_price) AS DECIMAL(10, 2)) AS net_sale
        FROM apex_bronze.retail_scans
        {% if is_incremental() %}
        WHERE to_date(to_timestamp(scan_datetime, 'yyyy-MM-dd HH:mm:ss')) = '{{ scan_date }}'
        {% endif %}
    ),
    final AS (
        SELECT
            scans.scan_id,
            scans.store_id,
            scans.scan_timestamp,
            scans.scan_datestamp,
            scans.item_upc,
            scans.unit_qty,
            scans.unit_price,
            scans.net_sale,
            items_categories.item_id,
            items_categories.category_code,
            items_categories.category_name,
            stores.city,
            stores.state,
            stores.region
        FROM scans
        INNER JOIN apex_bronze.retail_stores AS stores
            ON scans.store_id = stores.store_id
        INNER JOIN {{ ref('items_categories') }} AS items_categories
            ON scans.item_upc = items_categories.item_upc
    )
SELECT * FROM final