select
    event_id,
    order_id,
    customer_id,
    currency,
    line_number,
    product_id,
    quantity,
    unit_price,
    line_amount,
    occurred_at
from {{ ref('stg_order_items') }}
