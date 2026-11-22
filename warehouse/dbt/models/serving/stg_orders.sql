select
    event_id,
    order_id,
    customer_id,
    currency,
    total_amount,
    occurred_at,
    ingested_at,
    source
from {{ source('warehouse', 'wh_orders') }}
where warehouse_publication_id = {{ active_publication_id() }}
