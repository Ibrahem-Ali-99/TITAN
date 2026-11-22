select
    payment.payment_id,
    payment.order_id,
    payment.customer_id,
    payment.currency,
    payment.amount,
    payment.order_matched,
    orders.order_id is not null as order_found
from {{ ref('fct_payments') }} as payment
left join {{ ref('fct_orders') }} as orders
    on payment.order_id = orders.order_id
