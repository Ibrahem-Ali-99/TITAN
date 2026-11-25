import json
from pathlib import Path

from keycloak_fixture import USERS, KeycloakFixture


def test_keycloak_fixture_contract_is_non_secret() -> None:
    template = Path("tests/fixtures/keycloak/titan-realm.template.json")
    body = json.loads(template.read_text(encoding="utf-8"))
    assert body["realm"] == "titan-test"
    assert [user["username"] for user in body["users"]] == list(USERS)
    assert "${CLIENT_SECRET}" in template.read_text(encoding="utf-8")
    assert KeycloakFixture.__dataclass_fields__["client_secret_file"].type == "Path"
