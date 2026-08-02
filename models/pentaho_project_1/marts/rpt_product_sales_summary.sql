-- Replaces MERGE reporting.rpt_product_sales_summary.
-- dbt rebuilds the complete summary from the current product-sales model.
select
    product_id,
    max(product_name) as product_name,
    max(category) as category,
    count(distinct sales_id) as total_orders,
    sum(quantity) as total_quantity,
    cast(sum(sales_amount) as decimal(14, 2)) as total_sales_amount,
    min(sales_date) as first_sale_date,
    max(sales_date) as last_sale_date,
    current_timestamp() as updated_at
from {{ ref('stg_product_sales') }}
group by product_id

