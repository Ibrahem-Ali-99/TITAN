# Clean-clone release audit

Audit date: 2026-11-22. Starting commit: `61b60057a2198d49793669db4239f2aaf5a64139`. Workstation: Windows, PowerShell, Docker Compose v5. No push. `.cursor/mcp.json` was not used and was not committed.

**Verdict: PASSED for the isolated demonstration. Overall release stays CONDITIONAL.** A detached worktree published its own warehouse and the console showed the same figures. Hosted GitHub Actions and viewport widths 1024 and 768 were not run.

## Checkout

| Item | Value |
| --- | --- |
| Location | `D:\titan-clone` |
| Git | `git worktree add --detach D:\titan-clone 61b6005` |
| Compose project | `titan-clone` |
| Volumes | `titan-clone_*` only. Postgres mount `titan-clone_postgres_data`. |
| Development mount | `titan_postgres_data` stayed attached to `titan-postgres-1` |
| Development orders | 362 accepted `order_created` rows before the clone run and after the API rebuild |

No development `.env`, database dump, `.venv`, or `node_modules` was copied into the worktree. `.env` was created only by copying `.env.clone.example`. The shell variables `DATABASE_URL`, `CLICKHOUSE_PORT`, and `S3_ENDPOINT` left over from an older project were removed before the successful infrastructure check. The loaders use `setdefault`, so an inherited variable hides `.env`.

## Commands that reproduced the demo

From `D:\titan-clone`, after the documentation fixes were present in that tree:

```powershell
$env:COMPOSE_PROJECT_NAME = "titan-clone"
$env:COMPOSE_FILE = "docker-compose.yml;docker-compose.clone.yml"
copy .env.clone.example .env
docker compose up --build -d
docker compose run --rm --build migrate
uv sync --all-packages
uv sync --group orchestration
uv run python scripts/verify_infrastructure.py
uv run python -m event_generator --history-days 28 --seed 51022 --rate 0
docker compose --profile processing run --rm spark-bronze
docker compose --profile processing run --rm spark-silver
uv run --group orchestration python scripts/run_pipeline.py titan_warehouse
uv run --group orchestration python scripts/run_pipeline.py titan_ml
uv run python scripts/demo_day7.py
```

`COMPOSE_FILE` is not read from `.env`. It has to be set in the shell. `docker-compose.yml` sets `name: titan`. Without `COMPOSE_PROJECT_NAME`, a second directory attaches the development volumes. `.env.clone.example` uses `127.0.0.1` for `DATABASE_URL` because a `localhost` Postgres URL can spend the connect timeout on IPv6.

The same `verify_infrastructure.py` command, re-run after the demo with those inherited variables cleared, printed:

```text
postgres: ok SELECT 1 -> 1
kafka: ok brokers=[1]
clickhouse: ok authenticated SELECT 1
minio: ok authenticated ListBuckets count=1
volume: ok titan-clone_postgres_data
```

`docker inspect` showed `titan-clone_postgres_data` on the clone Postgres container and `titan_postgres_data` on the development container.

## Service health and the demonstration

| Check | Result |
| --- | --- |
| Compose | `titan-clone` containers started. Migrate exited 0. |
| PostgreSQL | Accepted rows after the 28-day backfill: 186 orders, 186 payments, 168 inventory, 0 quarantine |
| Kafka | Broker 1 on localhost:29092. The generator was acknowledged through inventory events. |
| ClickHouse | Authenticated `SELECT 1` on localhost:28123 |
| MinIO | Authenticated `ListBuckets` on localhost:29000. Count was 0 before the lake was used and 1 after. |
| FastAPI | `GET http://localhost:28000/health/ready` returned `status=ready`, `checks.postgres=ok` |
| Console | `http://localhost:23000` returned HTML 200 |
| Spark | Bronze then Silver exited 0 in 63s. Silver printed `1da76052-c405-493d-a8d4-a45e887e1b62`. Checkpoint volume `titan-clone_spark_checkpoints` was created. |
| Warehouse | `titan_warehouse` exited 0 in 39s and printed `82172430-e0a9-477a-9a5d-f66c1362cb2f`. dbt and the quality gate ran inside that command. |
| Dagster | `scripts/demo_day7.py` materializes through Dagster: one injected quality failure that must not move the pointer, then a clean publication. Exit 0 in 120s. |
| ML | Run `ff346763-282e-48b4-8323-fc998e909fe4`. Serving strategy `hist_gradient_boosting` because the candidate MAE was lower than `seasonal_naive`. Training publication `82172430-e0a9-477a-9a5d-f66c1362cb2f`. The demo did not replace that serving run. |
| API publication | After the demo: ready, publication `9cb0b695-4327-4ad5-b3db-f8a008177862`, 187 orders, EUR 6263.76, GBP 6664.47, USD 7448.35 |
| Console | Warehouse page text at 1440×900 and 390×844 showed 187 orders, 187 items, 187 payments, 169 inventory, €6,263.76, £6,664.47, US$7,448.35, publication `9cb0b695-4327-4ad5-b3db-f8a008177862` |

The demo order was `ord_af192b2bdemo`, event `a921bed9-2d0f-5a40-96da-0a3376e3a844`, EUR 19.95. Silver for that publication was `ca777410-93a2-41a8-a133-b79d1c1ba1bc`. Currencies were not added together.

Replay on the clone API: `POST /api/v1/ingestion/replays` with no token returned 401. The same call with `Bearer local-demo-token` and an empty body returned 422. Authentication stayed in front of body validation.

## Accessibility

`scripts/audit_a11y.py` with Playwright Chromium and axe-core 4.10.3. Base URL `http://localhost:23000`. Routes `/`, `/data-sources`, `/streaming`, `/pipelines`, `/warehouse`, `/data-quality`, `/ml`, `/monitoring`. Viewports 1440×900 and 390×844.

The first full run found serious `color-contrast` failures: `text-zinc-500` (`#71717b` on `#111d2b`, 3.52:1) on `/`, `/data-sources`, and `/ml`, and the default button `bg-sky-600` (`#0084d1` with white text, 4.06:1) on `/data-sources`. Tab on `/` focused an anchor with `outline: solid` and `outline-width: 2px` at both viewports.

After `text-zinc-400` and `bg-sky-700` (`#0369a1`, 5.93:1 with white), the clone console was restarted and the same script exited 0 with an empty violation list. Keyboard focus stayed on an anchor with a 2px solid outline. Widths 1024 and 768 were **NOT VERIFIED**.

## Security

| Check | Result |
| --- | --- |
| `uv run python scripts/scan_secrets.py` | Exit 0. Markers: `BEGIN PRIVATE KEY`, `BEGIN RSA PRIVATE KEY`, `AKIA`. |
| `uvx pip-audit` on `uv export --frozen --all-packages --group orchestration --no-emit-workspace` | Before the pin change: 12 rows, all `python-multipart` 0.0.20. After `==0.0.31`: `No known vulnerabilities found`, exit 0. |
| `npm audit --omit=dev` inside `titan-clone-web-1` | 0 vulnerabilities |
| Replay authentication | 401 without a token, 422 with the documented placeholder. The token was not emptied to obtain the 422. |
| Published ports | Workstation ports only. The clone uses 29092, 25435, 28123, 29000, 29001, 28000, and 23000 so it does not share the development ports. |
| Dagster socket | `dagster-daemon` mounts `/var/run/docker.sock` so the profile can run Compose jobs. That mount was left in place. Removing it would stop orchestration. |
| MinIO user | `user: "0:0"` is the container user required to initialize the named volume. It is not privileged mode. |

The multipart advisories on 0.0.20 included high-severity parser bugs that FastAPI reaches for form and multipart bodies (`PYSEC-2026-3036`, `PYSEC-2026-3039`), a medium preamble denial of service (`PYSEC-2026-3038`), and a high-severity file write that applies only when `UPLOAD_DIR` and `UPLOAD_KEEP_FILENAME` are set (`PYSEC-2026-1852`). The API pin is now `python-multipart==0.0.31`. The clone API image was rebuilt from this tree. The container reports version 0.0.31. Recreating that container from `D:\TITAN` interpolated this checkout's `.env`, so the container was recreated again from `D:\titan-clone` before the 401/422 check. Health and the 187-order publication were unchanged. The development order count stayed 362.

## Fixes

| Defect | Change | Regression check |
| --- | --- | --- |
| A second checkout attaches project `titan` | `docker-compose.clone.yml`, `.env.clone.example`, and a volume-name check in `scripts/verify_infrastructure.py` | `tests/unit/test_clone_isolation.py` |
| Pipeline modules are not installed packages | `scripts/run_pipeline.py` loads `.env` and the pipeline source trees | The clone warehouse and ML commands exited 0 |
| `demo_day7.py` required `seasonal_naive` | The assertion now requires the strategy already serving | The clone served `hist_gradient_boosting` and the script exited 0 |
| Inherited shell variables hide `.env` | Documented. `DATABASE_URL` in the clone example uses `127.0.0.1`. Postgres connect timeout is 15s. | `verify_infrastructure.py` passed against `127.0.0.1:25435` |
| Console contrast | `text-zinc-400` for muted text. Button `bg-sky-700` / `hover:bg-sky-800`. | axe-core, eight pages, two viewports, zero violations |
| `python-multipart` 0.0.20 | Pin `0.0.31` and refresh `uv.lock` | `pip-audit` clean. Clone API health ready on 0.0.31. |

## Checks after the fixes

| Command | Result |
| --- | --- |
| `uv run ruff check .` | Passed |
| `uv run ruff format --check .` | Passed. `scripts/audit_a11y.py` was wrapped. Two SQL string quotes in `scripts/verify_e2e_reliability.py` were reformatted with no text change. |
| `uv run mypy` | 83 files, no issues |
| `uv run pytest -m "not integration" -q` | 60 passed, 1 deselected, one Starlette `httpx` TestClient deprecation warning |
| `uv run python scripts/scan_secrets.py` | Exit 0 |
| `uvx pip-audit` on the frozen export | No known vulnerabilities found |
| `scripts/audit_a11y.py` against `http://localhost:23000` | Exit 0, zero violations |

## Still NOT VERIFIED

- A hosted GitHub Actions run for this commit.
- Console widths 1024 and 768.
- A machine with an empty Docker layer cache. The Spark image `titan-spark:4.0.1` and the API base layers were reused from this workstation.
- A live two-member Kafka rebalance and a MinIO outage during a write. Those limits stay in the README.
- `pytest -m integration` was not repeated in this session. The earlier isolated integration result is in `docs/audits/end-to-end-reliability.md`.

## Cleanup

After the evidence above, project `titan-clone` was stopped with `docker compose down` and without `-v`. Volumes `titan-clone_*` remain and can be removed later with `docker compose -p titan-clone down -v`. Development volumes `titan_*` were not removed. Projects `titan-e2e` and `titan-sparkkill` were not removed.
