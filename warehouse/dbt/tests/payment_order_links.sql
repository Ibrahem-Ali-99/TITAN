select payments.payment_id
from {{ ref('cand_payments') }} as payments
left join {{ ref('cand_orders') }} as orders
    on payments.order_id = orders.order_id
where payments.order_matched = 1 and orders.order_id is null
