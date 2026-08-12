-- Replaces the procedure's #incremental_sales customer lookup.
-- Every sale remains visible, including sales whose customer is missing.
select
    s.sales_id,
    s.order_number,
    s.customer_id,
    s.product_id,
    s.store_id,
    s.sales_date,
    s.quantity,
    s.unit_price,
    s.discount_amount,
    s.source_updated_at,
    c.customer_code,
    c.first_name,
    c.last_name,
    c.email as customer_email,
    c.region_id,
    c.is_active as customer_is_active,
    c.customer_id is null as is_missing_customer,
    p.product_id is null as is_missing_product
from {{ ref('stg_sales_source') }} as s
left join {{ ref('stg_customer_source') }} as c
    on s.customer_id = c.customer_id
left join {{ ref('stg_product_source') }} as p
    on s.product_id = p.product_id

