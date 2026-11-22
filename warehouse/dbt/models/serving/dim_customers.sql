select
    customer_id,
    count() as order_count,
    min(occurred_at) as first_order_at,
    max(occurred_at) as last_order_at
from {{ ref('fct_orders') }}
group by customer_id
