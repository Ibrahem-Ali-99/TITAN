import json
from pathlib import Path

from scripts.browser_oidc_smoke import write_evidence


def test_evidence_is_written_after_large_diagnostics(tmp_path: Path) -> None:
    path = tmp_path / "evidence.json"
    write_evidence(
        path,
        {
            "result": "failed",
            "redirects": ["https://idp.test/callback?code=secret&state=state-value"],
            "script_responses": [{"path": "/_next/app.js", "status": 200}],
            "large": "x" * 10000,
        },
    )
    body = json.loads(path.read_text(encoding="utf-8"))
    assert body["result"] == "failed"
    assert body["redirects"] == [
        "https://idp.test/callback?code=%5Bredacted%5D&state=%5Bredacted%5D"
    ]
    assert len(body["large"]) == 10000


def test_evidence_redacts_secret_fields(tmp_path: Path) -> None:
    path = tmp_path / "evidence.json"
    write_evidence(path, {"password": "pw", "client_secret": "secret", "nested": {"token": "tok"}})
    body = json.loads(path.read_text(encoding="utf-8"))
    assert body == {
        "password": "[redacted]",
        "client_secret": "[redacted]",
        "nested": {"token": "[redacted]"},
    }
