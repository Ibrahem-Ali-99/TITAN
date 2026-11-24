from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    database_url: str
    security_profile: str = Field(default="local", validation_alias="TITAN_SECURITY_PROFILE")
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
    metrics_collector_enabled: bool = True
    read_token: str | None = Field(default=None, validation_alias="TITAN_READ_TOKEN")
    write_token: str | None = Field(default=None, validation_alias="TITAN_WRITE_TOKEN")
    admin_token: str | None = Field(default=None, validation_alias="TITAN_ADMIN_TOKEN")

    @model_validator(mode="after")
    def validate_protected_profile(self) -> "ApiSettings":
        if self.security_profile.lower() not in {"protected", "production", "nonlocal"}:
            return self
        placeholders = {"", "titan_dev_password", "local-demo-token"}
        required = {
            "TITAN_READ_TOKEN": self.read_token,
            "TITAN_WRITE_TOKEN": self.write_token,
            "TITAN_ADMIN_TOKEN": self.admin_token,
            "CLICKHOUSE_PASSWORD": self.clickhouse_password,
        }
        missing = [
            name for name, value in required.items() if value is None or value in placeholders
        ]
        if missing:
            raise ValueError(
                "protected security profile requires non-placeholder secrets: " + ", ".join(missing)
            )
        return self
