select
    toDate(occurred_at) as activity_date,
    currency,
    count() as order_count,
    sum(total_amount) as accepted_revenue
from {{ ref('fct_orders') }}
group by activity_date, currency
