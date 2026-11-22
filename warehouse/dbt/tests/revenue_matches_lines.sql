select orders.order_id
from {{ ref('cand_orders') }} as orders
left join (
    select order_id, currency, sum(line_amount) as line_total
    from {{ ref('cand_order_items') }}
    group by order_id, currency
) as lines
    on orders.order_id = lines.order_id
    and orders.currency = lines.currency
where lines.line_total is null or orders.total_amount != lines.line_total
