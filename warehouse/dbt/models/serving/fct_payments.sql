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
from {{ ref('stg_payments') }}
