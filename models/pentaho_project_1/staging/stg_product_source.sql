-- One-to-one cleanup of dim.dim_product_source.
select
    cast(product_id as bigint) as product_id,
    cast(product_code as string) as product_code,
    trim(cast(product_name as string)) as product_name,
    trim(cast(category as string)) as category,
    cast(unit_price as decimal(10, 2)) as unit_price,
    cast(is_active as boolean) as is_active,
    cast(source_updated_at as timestamp) as source_updated_at
from {{ source('sql_server_dim', 'dim_product_source') }}

