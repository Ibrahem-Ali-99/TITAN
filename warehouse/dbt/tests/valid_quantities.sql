select event_id, line_number
from {{ ref('cand_order_items') }}
where quantity < 1 or unit_price < 0 or line_amount < 0
