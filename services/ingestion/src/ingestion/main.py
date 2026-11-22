import logging
import signal
from pathlib import Path

from ingestion.config import IngestionSettings
from ingestion.kafka_io import ConfluentConsumer
from ingestion.logging_config import configure_logging
from ingestion.runtime import run_consumer
from ingestion.store import SqlEventStore
from titan_messaging.admin import ensure_commerce_topics
from titan_store.db import create_db_engine, create_session_factory

logger = logging.getLogger(__name__)


def main() -> None:
    settings = IngestionSettings()
    configure_logging(settings.log_level)
    stop = False

    def request_stop(signum: int, _frame: object) -> None:
        nonlocal stop
        stop = True
        logger.info("shutdown requested", extra={"signal": signum})

    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is None:
            continue
        try:
            signal.signal(sig, request_stop)
        except (ValueError, OSError):
            logger.warning("could not register signal handler", extra={"signal": sig})

    heartbeat_path = Path(settings.ingestion_heartbeat_path)

    def heartbeat() -> None:
        heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
        heartbeat_path.write_text("ok", encoding="utf-8")

    topics = settings.topic_names()
    logger.info(
        "ingestion starting",
        extra={"topics": ",".join(topics), "group": settings.ingestion_consumer_group},
    )
    ensure_commerce_topics(settings.kafka_bootstrap_servers)
    engine = create_db_engine(settings.database_url)
    store = SqlEventStore(create_session_factory(engine))
    consumer = ConfluentConsumer(
        settings.kafka_bootstrap_servers,
        settings.ingestion_consumer_group,
        topics,
    )
    try:
        run_consumer(
            consumer,
            store,
            should_stop=lambda: stop,
            heartbeat=heartbeat,
            max_backoff_seconds=settings.ingestion_max_backoff_seconds,
        )
    finally:
        engine.dispose()
        logger.info("ingestion stopped")
