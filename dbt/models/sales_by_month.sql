-- Roll up sales by month, location, category

{{
    config(
        unique_key='item_upc'
    )
}}

SELECT 
    EXTRACT(YEAR from scan_timestamp) year, 
    EXTRACT(MONTH from scan_timestamp) month,
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
