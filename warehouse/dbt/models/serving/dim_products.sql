select
    product_id,
    sum(quantity) as units_sold
from {{ ref('fct_order_items') }}
group by product_id
