from pydantic_settings import BaseSettings, SettingsConfigDict


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    kafka_bootstrap_servers: str = "localhost:9092"
    cors_origins: str = "http://localhost:3000"
    log_level: str = "INFO"
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "titan"
    clickhouse_password: str = "titan_dev_password"
    clickhouse_database: str = "titan"
    otel_enabled: bool = False
    otel_exporter_otlp_endpoint: str = ""
