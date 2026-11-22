select
    customer_id,
    currency,
    count() as order_count,
    sum(total_amount) as revenue
from {{ ref('fct_orders') }}
group by customer_id, currency
