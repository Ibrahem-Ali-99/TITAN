select
    product_id,
    warehouse_id,
    quantity_on_hand,
    inventory_delta,
    occurred_at
from {{ ref('fct_inventory_events') }}
order by occurred_at desc
limit 1 by product_id, warehouse_id
