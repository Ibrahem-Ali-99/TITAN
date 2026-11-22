select
    event_id,
    product_id,
    warehouse_id,
    quantity_on_hand,
    inventory_delta,
    order_id,
    occurred_at
from {{ ref('stg_inventory') }}
