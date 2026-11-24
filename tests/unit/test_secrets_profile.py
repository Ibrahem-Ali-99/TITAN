import pytest
from pydantic import ValidationError

from titan_api.config import ApiSettings


def test_local_profile_keeps_development_defaults() -> None:
    settings = ApiSettings(database_url="sqlite://")
    assert settings.security_profile == "local"
    assert settings.clickhouse_password == "titan_dev_password"


def test_protected_profile_rejects_missing_and_placeholder_secrets() -> None:
    with pytest.raises(ValidationError, match="TITAN_READ_TOKEN"):
        ApiSettings(database_url="sqlite://", security_profile="protected")


def test_protected_profile_accepts_explicit_secrets() -> None:
    settings = ApiSettings(
        database_url="sqlite://",
        security_profile="protected",
        read_token="read-secret",
        write_token="write-secret",
        admin_token="admin-secret",
        clickhouse_password="clickhouse-secret",
    )
    assert settings.security_profile == "protected"


@pytest.mark.parametrize("profile", ["staging", "", " protected "])
def test_unknown_or_padded_profile_does_not_fall_back_to_local(profile: str) -> None:
    if profile == " protected ":
        with pytest.raises(ValidationError, match="TITAN_READ_TOKEN"):
            ApiSettings(database_url="sqlite://", security_profile=profile)
    else:
        with pytest.raises(ValidationError, match="unknown TITAN_SECURITY_PROFILE"):
            ApiSettings(database_url="sqlite://", security_profile=profile)


def test_placeholder_with_whitespace_is_rejected() -> None:
    with pytest.raises(ValidationError, match="TITAN_READ_TOKEN"):
        ApiSettings(
            database_url="sqlite://",
            security_profile="protected",
            read_token=" titan_dev_password ",
        )
