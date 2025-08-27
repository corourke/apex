-- Roll up sales by day, category, region

{{
    config(
        materialized='incremental',
        file_format='hudi',
        incremental_strategy='insert_overwrite',
        unique_key='scan_date,category_code,region',
        partition_by=['scan_date'],
        pre_hook=["SET hoodie.simple.index.parallelism=1000"],
        options={
            'hoodie.table.type': 'MERGE_ON_READ',
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.bulkinsert.sort.mode': 'NONE',
            'hoodie.datasource.write.partitionpath.field': 'scan_date',
            'primaryKey': 'scan_date,category_code,region'
        }
    )
}}

SELECT 
    DATE(scan_timestamp) AS scan_date, 
    category_code, 
    category_name,
    region, 
    sum(unit_qty) net_units,
    sum(net_sale) as net_sales
FROM {{ref('sales_detail')}}
GROUP BY 1, 2, 3, 4