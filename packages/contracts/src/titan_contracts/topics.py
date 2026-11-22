"""Kafka topic and consumer-group names for the Day 1 order stream."""

ORDER_EVENTS_TOPIC = "titan.commerce.order-events"
ORDER_EVENTS_PARTITIONS = 3
ORDER_EVENTS_RETENTION_MS = 7 * 24 * 60 * 60 * 1000
CONSUMER_GROUP = "titan-ingestion"
