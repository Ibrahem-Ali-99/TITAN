from pytest import MonkeyPatch

from titan_messaging.kafka_config import kafka_client_config


def test_plaintext_is_the_unchanged_development_default(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
    assert kafka_client_config("kafka:19092") == {"bootstrap.servers": "kafka:19092"}


def test_sasl_tls_requires_explicit_material(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "stage")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "secret")
    monkeypatch.delenv("KAFKA_SSL_CA_LOCATION", raising=False)
    try:
        kafka_client_config("kafka:19092")
    except ValueError as exc:
        assert "CA location" in str(exc)
    else:
        raise AssertionError("SASL/TLS without a CA must fail closed")


def test_sasl_tls_sets_verification(monkeypatch: MonkeyPatch) -> None:
    for name, value in {
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_SASL_MECHANISM": "PLAIN",
        "KAFKA_SASL_USERNAME": "stage",
        "KAFKA_SASL_PASSWORD": "secret",
        "KAFKA_SSL_CA_LOCATION": "/tls/ca.crt",
    }.items():
        monkeypatch.setenv(name, value)
    config = kafka_client_config("kafka:19092")
    assert config["security.protocol"] == "SASL_SSL"
    assert config["enable.ssl.certificate.verification"] is True
    assert config["ssl.endpoint.identification.algorithm"] == "https"
