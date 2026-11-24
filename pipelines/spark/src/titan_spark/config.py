"""Runtime settings. ``repr`` does not include credentials."""

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_security_protocol: str
    kafka_sasl_mechanism: str
    kafka_sasl_username: str
    kafka_sasl_password: str
    kafka_ssl_truststore_location: str
    kafka_ssl_truststore_password: str
    database_url: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    bucket: str
    checkpoint_root: str
    max_offsets_per_trigger: int
    window_duration: str
    watermark_delay: str

    def __repr__(self) -> str:
        return "Settings(redacted)"

    @property
    def bronze_uri(self) -> str:
        return f"s3a://{self.bucket}/bronze/commerce_events"

    @property
    def silver_root(self) -> str:
        return f"s3a://{self.bucket}/silver"

    @property
    def bronze_checkpoint(self) -> str:
        return f"{self.checkpoint_root}/bronze"

    @property
    def jdbc_url(self) -> str:
        without_driver = self.database_url.replace("postgresql+psycopg://", "postgresql://", 1)
        if "://" not in without_driver:
            raise ValueError("DATABASE_URL must be a postgresql URL")
        _, rest = without_driver.split("://", 1)
        if "@" not in rest:
            raise ValueError("DATABASE_URL must include credentials")
        host_and_db = rest.split("@", 1)[1]
        return f"jdbc:postgresql://{host_and_db}"

    @property
    def jdbc_user(self) -> str:
        return _credential(self.database_url)[0]

    @property
    def jdbc_password(self) -> str:
        return _credential(self.database_url)[1]

    def libpq_url(self) -> str:
        return self.database_url.replace("postgresql+psycopg://", "postgresql://", 1)


def load_settings() -> Settings:
    return Settings(
        kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
        kafka_security_protocol=_env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        kafka_sasl_mechanism=_env("KAFKA_SASL_MECHANISM", ""),
        kafka_sasl_username=_env("KAFKA_SASL_USERNAME", ""),
        kafka_sasl_password=os.environ.get("KAFKA_SASL_PASSWORD", ""),
        kafka_ssl_truststore_location=_env("KAFKA_SSL_TRUSTSTORE_LOCATION", ""),
        kafka_ssl_truststore_password=os.environ.get("KAFKA_SSL_TRUSTSTORE_PASSWORD", ""),
        database_url=_env(
            "DATABASE_URL",
            "postgresql://titan:titan_dev_password@postgres:5432/titan",
        ),
        s3_endpoint=_env("S3_ENDPOINT", "http://minio:9000"),
        s3_access_key=_env("S3_ACCESS_KEY", "titan"),
        s3_secret_key=_env("S3_SECRET_KEY", "titan_dev_password"),
        bucket=_env("LAKE_BUCKET", "titan-lake"),
        checkpoint_root=_env("CHECKPOINT_ROOT", "/var/lib/titan/spark-checkpoints"),
        max_offsets_per_trigger=int(_env("BRONZE_MAX_OFFSETS_PER_TRIGGER", "500")),
        window_duration=_env("ORDER_WINDOW_DURATION", "1 hour"),
        watermark_delay=_env("WATERMARK_DELAY", "10 minutes"),
    )


def _credential(database_url: str) -> tuple[str, str]:
    without_driver = database_url.replace("postgresql+psycopg://", "postgresql://", 1)
    rest = without_driver.split("://", 1)[1]
    userinfo = rest.split("@", 1)[0]
    user, _, password = userinfo.partition(":")
    return user, password
