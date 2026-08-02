-- One-to-one cleanup of dim.sales_source.
-- No business joins belong in this source-boundary model.
select
    cast(sales_id as bigint) as sales_id,
    cast(order_number as string) as order_number,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    cast(store_id as bigint) as store_id,
    cast(sales_date as date) as sales_date,
    cast(quantity as int) as quantity,
    cast(unit_price as decimal(10, 2)) as unit_price,
    cast(discount_amount as decimal(10, 2)) as discount_amount,
    cast(source_updated_at as timestamp) as source_updated_at
from {{ source('sql_server_dim', 'sales_source') }}

