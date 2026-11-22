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
from {{ source('warehouse', 'wh_order_items') }}
where warehouse_publication_id = {{ active_publication_id() }}
