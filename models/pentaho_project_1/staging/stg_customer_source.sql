-- One-to-one cleanup of dim.dim_customer_source.
select
    cast(customer_id as bigint) as customer_id,
    cast(customer_code as string) as customer_code,
    trim(cast(first_name as string)) as first_name,
    trim(cast(last_name as string)) as last_name,
    lower(trim(cast(email as string))) as email,
    cast(region_id as bigint) as region_id,
    cast(is_active as boolean) as is_active,
    cast(source_updated_at as timestamp) as source_updated_at
from {{ source('sql_server_dim', 'dim_customer_source') }}

