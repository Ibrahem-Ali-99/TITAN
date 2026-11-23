import subprocess
import sys
from pathlib import Path


def test_supply_chain_policy_passes() -> None:
    script = Path(__file__).resolve().parents[2] / "scripts" / "check_supply_chain.py"
    result = subprocess.run(
        [sys.executable, str(script)], check=False, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stdout + result.stderr
