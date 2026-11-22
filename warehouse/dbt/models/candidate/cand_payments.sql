select
    event_id,
    payment_id,
    order_id,
    customer_id,
    currency,
    amount,
    order_matched,
    occurred_at
from {{ source('warehouse', 'wh_payments') }}
{{ candidate_filter('warehouse_publication_id') }}
