{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='sales_id',
        schema='staging'
    )
}}

-- Replaces MERGE staging.stg_product_sales.
-- An inner join reproduces the procedure's requirement for a valid product.
select
    s.sales_id,
    s.order_number,
    s.product_id,
    p.product_name,
    p.category,
    s.store_id,
    s.sales_date,
    s.quantity,
    cast((s.quantity * s.unit_price) - s.discount_amount as decimal(12, 2)) as sales_amount,
    s.source_updated_at,
    current_timestamp() as loaded_at
from {{ ref('int_sales_customer_status') }} as s
inner join {{ ref('stg_product_source') }} as p
    on s.product_id = p.product_id
where not s.is_missing_customer and not s.is_missing_product
