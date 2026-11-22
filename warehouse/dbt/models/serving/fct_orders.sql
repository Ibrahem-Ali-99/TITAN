select
    event_id,
    order_id,
    customer_id,
    currency,
    total_amount,
    occurred_at,
    ingested_at,
    source
from {{ ref('stg_orders') }}
