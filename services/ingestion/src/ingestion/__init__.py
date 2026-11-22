"""At-least-once Kafka consumer with idempotent PostgreSQL persistence.

Offset commits happen only after a durable database commit. Re-delivery of
the same ``event_id`` does not create another business row. This is not an
end-to-end exactly-once guarantee.
"""
