-- Sales inputs must describe a positive quantity and price, with a discount that
-- cannot make the calculated sales amount negative.
select sales_id
from {{ ref('stg_sales_source') }}
where quantity <= 0
   or unit_price < 0
   or discount_amount < 0
   or discount_amount > quantity * unit_price
