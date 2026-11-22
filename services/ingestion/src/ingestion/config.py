from pydantic_settings import BaseSettings, SettingsConfigDict

from titan_contracts.topics import COMMERCE_TOPICS, CONSUMER_GROUP


class IngestionSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    kafka_bootstrap_servers: str = "localhost:9092"
    ingestion_consumer_group: str = CONSUMER_GROUP
    ingestion_topics: str = ""
    log_level: str = "INFO"
    ingestion_heartbeat_path: str = "/tmp/ingestion-heartbeat"
    ingestion_max_backoff_seconds: float = 30.0

    def topic_names(self) -> list[str]:
        configured = [item.strip() for item in self.ingestion_topics.split(",") if item.strip()]
        if configured:
            return configured
        return [spec.name for spec in COMMERCE_TOPICS]
