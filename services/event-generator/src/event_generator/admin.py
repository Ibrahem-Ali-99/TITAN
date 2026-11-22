"""Topic setup for the generator. Delegates to the shared messaging package."""

from titan_messaging.admin import ensure_commerce_topics


def ensure_order_topic(bootstrap_servers: str, *, attempts: int = 5) -> None:
    ensure_commerce_topics(bootstrap_servers, attempts=attempts)
