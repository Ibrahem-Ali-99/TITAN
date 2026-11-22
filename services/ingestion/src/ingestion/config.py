from pydantic_settings import BaseSettings, SettingsConfigDict

from titan_contracts.topics import CONSUMER_GROUP, ORDER_EVENTS_TOPIC


class IngestionSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    kafka_bootstrap_servers: str = "localhost:9092"
    ingestion_consumer_group: str = CONSUMER_GROUP
    ingestion_topic: str = ORDER_EVENTS_TOPIC
    log_level: str = "INFO"
    ingestion_heartbeat_path: str = "/tmp/ingestion-heartbeat"
    ingestion_max_backoff_seconds: float = 30.0
