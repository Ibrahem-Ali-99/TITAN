# Ingestion hub

Day 2 adds REST and CSV inputs. Both publish canonical events to Kafka. The consumer remains the only writer of `commerce_events`.

## Identity

`event_id` identifies one delivery of a fact. `order_id`, `payment_id`, and `product_id` identify the business entity. A repeated `event_id` is a duplicate delivery: one stored envelope, plus one `ingestion_observations` row per Kafka position. A new `event_id` for an order or payment that is already stored is not another accepted order or payment.

`occurred_at` is the business time supplied by the producer. `ingested_at` is the database write time. `produced_at` is when the producer built the envelope.

## Conflicts

The first durable `order_created` for an `order_id` is `accepted`. A later event with the same payload fingerprint is `duplicate_operation`. A later event with a different payload is `conflict`. Both are stored as evidence. `logical_orders` counts only `accepted` rows. Inventory updates are state changes: each new `event_id` is `accepted`, even when the `product_id` repeats. `order_id` is not unique across event types.

The accepted row is the first one persisted, not necessarily the earliest `occurred_at`.

## Schema and money

Schema version 1 is the only accepted version for `order_created`, `payment_completed`, and `inventory_updated`. Other versions and event types are quarantined. Money stays decimal strings in USD, EUR, or GBP with no silent rounding. A CSV order row has one line; the total is the exact line amount.

## Quarantine and replay

`ingestion_quarantine` is the dead-letter record. There is no Kafka dead-letter topic, so acknowledgement waits on that database insert alone. Exact replay republishes a stored valid envelope with the same `event_id`. A malformed record or a key mismatch is refused until an operator submits a replacement with a new `event_id`. Replay batches are limited to 20. `outcome=published` means Kafka acknowledged the replay. `downstream_state=persisted` means the consumer has stored that `event_id`.

## CSV

Headers, in order:

`event_type,occurred_at,order_id,customer_id,currency,product_id,quantity,unit_price,payment_id,amount,payment_method,quantity_on_hand,inventory_delta,warehouse_id`

Limits are 500 data rows and 1 MiB. The request validates the file and returns. Kafka publication continues in the API process. A restart can leave rows in `publishing`; `POST /api/v1/ingestion/imports/{id}/publish` resumes them.

## Metrics

Every stats field has a `metric_definitions` entry naming its numerator and source table. Persisted counts, REST publication counts, and CSV publication counts are different measurements. None of them are Kafka broker throughput or consumer lag.
