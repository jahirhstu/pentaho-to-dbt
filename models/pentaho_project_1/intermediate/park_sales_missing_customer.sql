{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='sales_id',
        schema='parking'
    )
}}

-- Replaces the read/write parking table with a dbt-owned Delta table.
-- A row first appears when its customer is missing. On later runs the same row is
-- retained and marked resolved when that customer becomes available.
with current_sales as (

    select *
    from {{ ref('int_sales_customer_status') }}

),

{% if is_incremental() %}

previous_exceptions as (

    select
        sales_id,
        parked_at,
        resolved_at
    from {{ this }}

),

exception_candidates as (

    select
        current_sales.*,
        previous_exceptions.parked_at as previous_parked_at,
        previous_exceptions.resolved_at as previous_resolved_at
    from current_sales
    left join previous_exceptions
        on current_sales.sales_id = previous_exceptions.sales_id
    where current_sales.is_missing_customer
       or previous_exceptions.sales_id is not null

)

{% else %}

exception_candidates as (

    select
        current_sales.*,
        cast(null as timestamp) as previous_parked_at,
        cast(null as timestamp) as previous_resolved_at
    from current_sales
    where current_sales.is_missing_customer

)

{% endif %}

select
    sales_id,
    order_number,
    customer_id,
    product_id,
    store_id,
    sales_date,
    quantity,
    unit_price,
    discount_amount,
    source_updated_at,
    'Missing customer_id in dim.dim_customer_source' as rejection_reason,
    coalesce(previous_parked_at, current_timestamp()) as parked_at,
    not is_missing_customer as is_resolved,
    case
        when not is_missing_customer
            then coalesce(previous_resolved_at, current_timestamp())
        else cast(null as timestamp)
    end as resolved_at
from exception_candidates

