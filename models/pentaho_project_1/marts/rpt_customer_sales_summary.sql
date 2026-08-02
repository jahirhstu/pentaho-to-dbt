-- Replaces MERGE reporting.rpt_customer_sales_summary.
-- dbt rebuilds the complete summary from the current customer-sales model.
select
    customer_id,
    max(customer_name) as customer_name,
    max(customer_email) as customer_email,
    max(region_id) as region_id,
    count(distinct sales_id) as total_orders,
    sum(quantity) as total_quantity,
    cast(sum(sales_amount) as decimal(14, 2)) as total_sales_amount,
    min(sales_date) as first_sale_date,
    max(sales_date) as last_sale_date,
    current_timestamp() as updated_at
from {{ ref('stg_customer_sales') }}
group by customer_id

