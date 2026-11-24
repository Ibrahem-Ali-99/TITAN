import os
import stat
from pathlib import Path

import pytest

from dr_security import secure_tree


@pytest.mark.skipif(os.name == "nt", reason="Windows ACLs are not represented by mode bits")
def test_backup_tree_uses_private_posix_modes(tmp_path: Path) -> None:
    root = tmp_path / "backup"
    payload = root / "objects" / "event.bin"
    payload.parent.mkdir(parents=True)
    payload.write_bytes(b"secret")

    secure_tree(root)

    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE((root / "objects").stat().st_mode) == 0o700
    assert stat.S_IMODE(payload.stat().st_mode) == 0o600
