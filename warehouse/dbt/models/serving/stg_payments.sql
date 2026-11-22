select
    event_id,
    payment_id,
    order_id,
    customer_id,
    currency,
    amount,
    method,
    order_matched,
    occurred_at
from {{ source('warehouse', 'wh_payments') }}
where warehouse_publication_id = {{ active_publication_id() }}
