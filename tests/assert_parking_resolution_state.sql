-- Unresolved exceptions have no resolution timestamp; resolved exceptions have one.
select sales_id
from {{ ref('park_sales_missing_customer') }}
where (is_resolved and resolved_at is null)
   or (not is_resolved and resolved_at is not null)
   or resolved_at < parked_at
