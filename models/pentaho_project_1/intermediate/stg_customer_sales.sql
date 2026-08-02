{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='sales_id',
        schema='staging'
    )
}}

-- Replaces MERGE staging.stg_customer_sales.
-- The full valid input is selected for clarity; dbt merges it by sales_id.
select
    sales_id,
    order_number,
    customer_id,
    concat(first_name, ' ', last_name) as customer_name,
    customer_email,
    region_id,
    sales_date,
    quantity,
    cast((quantity * unit_price) - discount_amount as decimal(12, 2)) as sales_amount,
    source_updated_at,
    current_timestamp() as loaded_at
from {{ ref('int_sales_customer_status') }}
where not is_missing_customer

