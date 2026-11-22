# Release-candidate verification

Audit date: 2026-11-22. Candidate: `6ebd47dfd05e7304ff0b601d6339fd1ff47835d1` (`fix: reproduce the demo from an isolated checkout`). Detached worktree: `D:\titan-rc`. Workstation: Windows, PowerShell, Docker Compose v5. No remote was added. Nothing was pushed, deployed, or published.

**Verdict: CONDITIONAL.** The candidate reproduces its local checks, and the cold image builds succeeded. Hosted GitHub Actions are **NOT VERIFIED**. Publishing them requires an explicit remote and a push.

## 1. Verified commit and working tree

| Check | Result |
| --- | --- |
| `git rev-parse HEAD` in `D:\TITAN` before edits | `6ebd47dfd05e7304ff0b601d6339fd1ff47835d1` |
| Status at that point | Clean except untracked `.cursor/mcp.json` |
| Detached checkout | `git worktree add --detach D:\titan-rc 6ebd47d`. `git status` there was clean. |
| Clean-clone files in the commit | `.env.clone.example`, `docker-compose.clone.yml`, `docs/audits/clean-clone-release.md`, `scripts/run_pipeline.py`, `scripts/audit_a11y.py` |
| Excluded | `.env` is ignored. `.venv/`, `__pycache__/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `node_modules/`, `.next/`, and `warehouse/dbt/target/` are ignored. Docker volumes are not files in Git. `.cursor/mcp.json` was untracked and is now ignored. Rules and skills under `.cursor/` stay tracked. |
| Author of `6ebd47d` | Ibrahem-Ali-99 |

The repository has no `git remote`.

## 2. GitHub Actions

One workflow: `.github/workflows/ci.yml`.

| Job | Steps at `6ebd47d` | Change in this verification |
| --- | --- | --- |
| `python` | frozen `uv sync`, Ruff lint, Ruff format, mypy, `pytest -m "not integration"`, secret scan | Added a frozen export of third-party requirements and `pip-audit` 2.10.1 |
| `orchestration` | frozen orchestration sync, `dagster definitions validate` with `DAGSTER_HOME` and `PYTHONPATH` | Unchanged |
| `web` | Node 26.13.1, npm cache, `npm ci`, test, ESLint, `tsc --noEmit`, Next.js build | Added `npm audit --omit=dev` before the tests |

`permissions: contents: read`. Triggers are `push` and `pull_request`. A failed step fails the job. There is no `continue-on-error`. uv is pinned to 0.12.1. The web lockfile is `apps/web/package-lock.json`. Actions are `actions/checkout@v4`, `astral-sh/setup-uv@v5`, and `actions/setup-node@v4`. Those are major-version tags, not commit SHAs.

**Hosted CI status: NOT VERIFIED.** No workflow run exists for this commit because there is no remote.

## 3. Local CI-equivalent results

Commands in `D:\titan-rc` at `6ebd47d`, except the web commands, which ran in `titan-web-1` against the bind-mounted tree while that tree was still `6ebd47d`.

| Command | Exit | Result |
| --- | --- | --- |
| `uv run ruff check` | 0 | All checks passed |
| `uv run ruff format --check` | 0 | 137 files already formatted |
| `uv run mypy` | 0 | 83 source files |
| `uv run pytest -m "not integration" -q` | 0 | 60 passed, 1 deselected |
| `uv run python scripts/scan_secrets.py` | 1 | The clean-clone audit quoted the scanner's own needles. See findings. |
| `dagster definitions validate` | 0 | `Validation successful for code location titan_dagster.definitions:defs.` |
| `uvx pip-audit` 2.10.1 on the frozen third-party export | 0 | No known vulnerabilities found |
| `npm test` in the web container | 0 | 10 passed, 0 failed. Node printed `MODULE_TYPELESS_PACKAGE_JSON`. `type: module` was not added. |
| `npm run lint` | 0 | ESLint produced no findings |
| `npx tsc --noEmit` | 0 | No output |
| `npm audit --omit=dev` | 0 | 0 vulnerabilities |
| `npm run build` | 0 | Next.js 16.4.6, 11 static routes |

Pytest warning, left in place: Starlette `httpx` TestClient deprecation, suggesting `httpx2`.

After the documentation fix and the new secret-scan test, from `D:\TITAN`:

| Command | Exit | Result |
| --- | --- | --- |
| `uv run ruff check .` | 0 | Passed |
| `uv run ruff format --check .` | 0 | 138 files already formatted |
| `uv run mypy` | 0 | 84 source files |
| `uv run pytest -m "not integration" -q` | 0 | 61 passed, 1 deselected, same Starlette warning |
| `uv run python scripts/scan_secrets.py` | 0 | No output |

`pytest -m integration` was not repeated. Its earlier result stays in `docs/audits/end-to-end-reliability.md`.

## 4. Docker clean build

Built from `D:\titan-rc` with `--no-cache --pull`. Tags do not replace `titan-spark:4.0.1` or the running `titan-*` images. Development volumes were not removed. Accepted orders on `titan-postgres-1` stayed 362, and the seven development containers stayed up.

| Image | Exit |
| --- | --- |
| `titan-rc-python:6ebd47d` | 0 |
| `titan-rc-web:6ebd47d` | 0 |
| `titan-rc-orchestration:6ebd47d` | 0 |
| `titan-rc-spark:6ebd47d` | 0. The Spark archive checksum printed `OK` during the build. |

`docker pull` for `apache/kafka:4.4.0`, `postgres:18.7-alpine3.23`, `clickhouse/clickhouse-server:26.8.36.3`, and `alpine/minio:RELEASE.2025-10-15T17-29-55Z` exited 0 with `Image is up to date`. Those pulls confirmed the pins. They did not start from an empty Docker cache.

The web image command is `npm run dev`. The production build is the CI `npm run build`, not the image process. The orchestration image installs from `uv.lock` with `--frozen`. The Python image does the same and drops to user `titan`.

## 5. Responsive and accessibility results

`scripts/audit_a11y.py` with Playwright Chromium and axe-core 4.10.3 against `http://localhost:3000`. Eight routes. Widths 1440×900, 1024×768, 768×1024, and 390×844. Each navigation waited for `networkidle`.

The first run reported serious `color-contrast` violations on `/`, `/data-sources`, and `/ml`. The committed source no longer uses `text-zinc-500` or `bg-sky-600`. The development console was still serving the previous bundle. After `docker restart titan-web-1`, the same script exited 0 with an empty violation list.

At all four widths the passing run recorded:

- Tab on `/` focused the Overview link, with a solid 2px outline.
- Eight primary nav links and an `h1` on every page. No horizontal overflow beyond 1px. No console or page errors. No page left on a loading state.
- CSV file input accessible name `CSV file`, `required`, and `reportValidity()` blocked an empty upload.
- Warehouse charts: 5 SVG charts. Visible figures: 362 orders, 540 items, 193 payments, 175 inventory, €18,711.58, £16,558.03, US$17,768.77.
- Tables that were present had header cells. Hint buttons expose their text through `aria-label`.

## 6. Security

| Check | Result |
| --- | --- |
| Secret scan on `6ebd47d` | Failed on the audit document, not on a credential. Fixed by removing the quoted needles. `tests/unit/test_secret_scan.py` now runs the scanner. |
| `pip-audit` 2.10.1 | No known vulnerabilities in the frozen third-party export, including `python-multipart` 0.0.31 |
| `npm audit --omit=dev` | 0 vulnerabilities |
| Replay authentication | Unchanged. An empty admin token stays fail-closed. The placeholder token was not removed. |
| Compose exposure | Workstation ports, documented local passwords, Dagster's Docker socket, and MinIO `user: "0:0"` for the named volume. None of those were widened. |
| Delivery claim | README and the audits say at-least-once with idempotent persistence. They do not claim end-to-end exactly-once or production readiness. |

## 7. Findings

| Severity | Finding | Disposition |
| --- | --- | --- |
| Major | `scan_secrets.py` exits 1 on `6ebd47d` because `docs/audits/clean-clone-release.md` quoted the scanner needles. CI would fail. | Fixed. Regression test `test_tracked_files_do_not_contain_secret_markers`. |
| Minor | The first accessibility run failed on stale console CSS. The committed styles were already corrected. | Restarted `titan-web-1`. Re-run exited 0. No further style change. |
| Suggestion | Actions float on major tags, and a fresh machine still needs the upstream image digests already present here. | Left as limitations. |

No Critical defect was found in this pass.

## 8. Remaining limitations

- Hosted GitHub Actions for `6ebd47d` and for the commit that contains these fixes: **NOT VERIFIED**.
- An empty Docker daemon was not used. The four project images were rebuilt with `--no-cache --pull`. The four upstream images were already local.
- `pytest -m integration` was not repeated.
- A live two-member Kafka rebalance and a MinIO outage during a write remain unexecuted.
- The web container runs the Next.js dev server. Node's module-type warning and the Starlette TestClient warning remain.
- Dagster still mounts the host Docker socket. That is required for the Compose-backed jobs.

## 9. What needs authorization

Creating a GitHub repository, adding a remote, and pushing `master` are not done. Those steps are what a hosted workflow run requires. Say so explicitly before either happens.
