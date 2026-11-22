create table if not exists wh_orders (
    warehouse_publication_id String,
    event_id String,
    order_id String,
    customer_id String,
    currency LowCardinality(String),
    total_amount Decimal(18, 2),
    occurred_at DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC'),
    source String
) engine = MergeTree
order by (warehouse_publication_id, currency, occurred_at, order_id);

create table if not exists wh_order_items (
    warehouse_publication_id String,
    event_id String,
    order_id String,
    customer_id String,
    currency LowCardinality(String),
    line_number Int32,
    product_id String,
    quantity Int32,
    unit_price Decimal(18, 2),
    line_amount Decimal(18, 2),
    occurred_at DateTime64(6, 'UTC')
) engine = MergeTree
order by (warehouse_publication_id, order_id, line_number);

create table if not exists wh_payments (
    warehouse_publication_id String,
    event_id String,
    payment_id String,
    order_id String,
    customer_id String,
    currency LowCardinality(String),
    amount Decimal(18, 2),
    method LowCardinality(String),
    order_matched UInt8,
    occurred_at DateTime64(6, 'UTC')
) engine = MergeTree
order by (warehouse_publication_id, order_id, payment_id);

create table if not exists wh_inventory (
    warehouse_publication_id String,
    event_id String,
    product_id String,
    warehouse_id String,
    quantity_on_hand Int32,
    inventory_delta Int32,
    order_id Nullable(String),
    occurred_at DateTime64(6, 'UTC')
) engine = MergeTree
order by (warehouse_publication_id, product_id, warehouse_id, occurred_at, event_id);

create table if not exists wh_active (
    warehouse_publication_id String,
    silver_publication_id String,
    published_at DateTime64(6, 'UTC'),
    order_count UInt64,
    payment_count UInt64,
    inventory_count UInt64,
    item_count UInt64,
    revenue_json String
) engine = MergeTree
order by (warehouse_publication_id);
