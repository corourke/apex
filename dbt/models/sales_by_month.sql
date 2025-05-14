-- Roll up sales by month, location, category

{{
    config(
        materialized='incremental',
        file_format='hudi',
        incremental_strategy='merge',
        unique_key='item_upc',
        location_root='s3://onehouse-customer-bucket-7a00bf9c/datalake/',
        schema='apex_silver',
        options={
        'type': 'mor',
        'primaryKey': 'item_upc',
        'precombineKey': 'event_timestamp'
        }
    )
}}

SELECT 
    EXTRACT(YEAR from scan_datetime) year, 
    EXTRACT(MONTH from scan_datetime) month,
    year_month,
    item_upc,
    category_code, 
    category_name,
    store_id, 
    city,
    "state",
    region, 
    sum(unit_qty) net_units,
    sum(net_sale) as net_sales
FROM {{ref('sales_detail')}}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
