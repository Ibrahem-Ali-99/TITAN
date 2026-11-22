select
    product_id,
    currency,
    sum(quantity) as quantity,
    sum(line_amount) as revenue
from {{ ref('fct_order_items') }}
group by product_id, currency
