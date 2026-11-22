select
    toStartOfHour(occurred_at) as window_start,
    currency,
    count() as order_count,
    sum(total_amount) as accepted_revenue
from {{ ref('fct_orders') }}
group by window_start, currency
