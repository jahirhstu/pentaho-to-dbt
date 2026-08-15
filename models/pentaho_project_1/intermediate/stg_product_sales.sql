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
-- depends_on: {{ ref('stg_product_source') }}
-- depends_on: {{ ref('park_sales_missing_customer') }}
-- depends_on: {{ ref('park_sales_missing_product') }}

-- Replaces MERGE staging.stg_product_sales.
-- Incremental runs process direct sales changes, customer/product lookup changes,
-- and parked sales recovered by a lookup arriving in the same source window.
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

changed_products as (

    select product_id
    from {{ ref('stg_product_source') }}
    where source_updated_at > cast('{{ var("product_watermark_start") }}' as timestamp)
      and source_updated_at <= cast('{{ var("product_watermark_end") }}' as timestamp)

),

sales_affected_by_customers as (

    select sales.sales_id
    from {{ ref('stg_sales_source') }} as sales
    inner join changed_customers
        on sales.customer_id = changed_customers.customer_id

),

sales_affected_by_products as (

    select sales.sales_id
    from {{ ref('stg_sales_source') }} as sales
    inner join changed_products
        on sales.product_id = changed_products.product_id

),

recovered_customer_sales as (

    select parked.sales_id
    from {{ ref('park_sales_missing_customer') }} as parked
    inner join {{ ref('stg_customer_source') }} as customer
        on parked.customer_id = customer.customer_id
    where customer.source_updated_at > cast('{{ var("customer_watermark_start") }}' as timestamp)
      and customer.source_updated_at <= cast('{{ var("customer_watermark_end") }}' as timestamp)

),

recovered_product_sales as (

    select parked.sales_id
    from {{ ref('park_sales_missing_product') }} as parked
    inner join {{ ref('stg_product_source') }} as product
        on parked.product_id = product.product_id
    where product.source_updated_at > cast('{{ var("product_watermark_start") }}' as timestamp)
      and product.source_updated_at <= cast('{{ var("product_watermark_end") }}' as timestamp)

),

affected_sales as (

    select sales_id from changed_sales
    union
    select sales_id from sales_affected_by_customers
    union
    select sales_id from sales_affected_by_products
    union
    select sales_id from recovered_customer_sales
    union
    select sales_id from recovered_product_sales

),

{% endif %}

current_product_sales as (

    select
        sales.sales_id,
        sales.order_number,
        sales.product_id,
        product.product_name,
        product.category,
        sales.store_id,
        sales.sales_date,
        sales.quantity,
        cast((sales.quantity * sales.unit_price) - sales.discount_amount as decimal(12, 2)) as sales_amount,
        sales.source_updated_at
    from {{ ref('int_sales_customer_status') }} as sales
    inner join {{ ref('stg_product_source') }} as product
        on sales.product_id = product.product_id
    where not sales.is_missing_customer
      and not sales.is_missing_product

    {% if is_incremental() %}
      and sales.sales_id in (select sales_id from affected_sales)
    {% endif %}

)

select
    *,
    current_timestamp() as loaded_at
from current_product_sales
