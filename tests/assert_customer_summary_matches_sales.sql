-- Each customer summary must equal a fresh aggregation of its enriched sales rows.
with expected as (

    select
        customer_id,
        count(distinct sales_id) as total_orders,
        sum(quantity) as total_quantity,
        cast(sum(sales_amount) as decimal(14, 2)) as total_sales_amount,
        min(sales_date) as first_sale_date,
        max(sales_date) as last_sale_date
    from {{ ref('stg_customer_sales') }}
    group by customer_id

)

select actual.customer_id
from {{ ref('rpt_customer_sales_summary') }} as actual
full outer join expected
    on actual.customer_id = expected.customer_id
where actual.customer_id is null
   or expected.customer_id is null
   or actual.total_orders <> expected.total_orders
   or actual.total_quantity <> expected.total_quantity
   or actual.total_sales_amount <> expected.total_sales_amount
   or actual.first_sale_date <> expected.first_sale_date
   or actual.last_sale_date <> expected.last_sale_date
