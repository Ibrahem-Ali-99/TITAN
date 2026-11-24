"""Run certificate tooling in the pinned Linux staging tool image."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

TOOL_IMAGE = "titan-spark:protected-f3952feb443145ed90c19e64a6ef433b502f68ca"
WINDOWS_PATH = re.compile(r"^[A-Za-z]:[\\/]")


def run(args: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[bytes]:
    if args and args[0] == "docker":
        return subprocess.run(args, check=True, capture_output=True)
    paths: list[Path] = []
    for value in args:
        if WINDOWS_PATH.match(value):
            path = Path(value)
            paths.append(path if path.is_dir() else path.parent)
    if cwd is not None:
        paths.append(cwd.resolve())
    roots: list[Path] = []
    for path in paths:
        resolved = path.resolve()
        if not any(resolved == root or root in resolved.parents for root in roots):
            roots.append(resolved)
    rewritten: list[str] = []
    for value in args:
        replacement = value
        if WINDOWS_PATH.match(value):
            resolved = Path(value).resolve()
            for index, root in enumerate(roots):
                if resolved == root or root in resolved.parents:
                    replacement = f"/work{index}/{resolved.relative_to(root).as_posix()}"
                    break
        rewritten.append(replacement)
    command = ["docker", "run", "--rm", "--entrypoint", "bash"]
    for index, root in enumerate(roots):
        command.extend(("-v", f"{root}:/work{index}"))
    shell = 'exec "$1" "${@:2}"'
    command.extend((TOOL_IMAGE, "-c", shell, "staging-tool", rewritten[0], *rewritten[1:]))
    return subprocess.run(command, check=True, capture_output=True)
