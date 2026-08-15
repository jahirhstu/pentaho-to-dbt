{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='sales_id',
        schema='staging'
    )
}}

-- depends_on: {{ ref('stg_sales_source') }}
-- depends_on: {{ ref('stg_customer_source') }}
-- depends_on: {{ ref('park_sales_missing_customer') }}

-- Replaces MERGE staging.stg_customer_sales.
-- Incremental runs process sales changed directly, sales affected by customer
-- changes, and parked sales recovered by a customer arriving in the same window.
with

{% if is_incremental() %}

changed_sales as (

    select sales_id
    from {{ ref('stg_sales_source') }}
    where source_updated_at > cast('{{ var("sales_watermark_start") }}' as timestamp)
      and source_updated_at <= cast('{{ var("sales_watermark_end") }}' as timestamp)

),

changed_customers as (

    select customer_id
    from {{ ref('stg_customer_source') }}
    where source_updated_at > cast('{{ var("customer_watermark_start") }}' as timestamp)
      and source_updated_at <= cast('{{ var("customer_watermark_end") }}' as timestamp)

),

sales_affected_by_customers as (

    select sales.sales_id
    from {{ ref('stg_sales_source') }} as sales
    inner join changed_customers
        on sales.customer_id = changed_customers.customer_id

),

recovered_customer_sales as (

    select parked.sales_id
    from {{ ref('park_sales_missing_customer') }} as parked
    inner join {{ ref('stg_customer_source') }} as customer
        on parked.customer_id = customer.customer_id
    where customer.source_updated_at > cast('{{ var("customer_watermark_start") }}' as timestamp)
      and customer.source_updated_at <= cast('{{ var("customer_watermark_end") }}' as timestamp)

),

affected_sales as (

    select sales_id from changed_sales
    union
    select sales_id from sales_affected_by_customers
    union
    select sales_id from recovered_customer_sales

),

{% endif %}

current_customer_sales as (

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
        source_updated_at
    from {{ ref('int_sales_customer_status') }}
    where not is_missing_customer

    {% if is_incremental() %}
      and sales_id in (select sales_id from affected_sales)
    {% endif %}

)

select
    *,
    current_timestamp() as loaded_at
from current_customer_sales
