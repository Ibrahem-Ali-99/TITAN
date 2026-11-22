select
    event_id,
    order_id,
    customer_id,
    currency,
    total_amount,
    occurred_at
from {{ source('warehouse', 'wh_orders') }}
{{ candidate_filter('warehouse_publication_id') }}
