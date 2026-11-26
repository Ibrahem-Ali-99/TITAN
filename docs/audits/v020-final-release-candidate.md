# TITAN v0.2.0 final release-candidate verification

Verification date: 2026-11-26 (Africa/Cairo). Repository: `D:\TITAN`. Branch: `feature/titan-v0.2.0-final-ui`.

This record closes the targeted release-quality gaps from the final review. The work was local-only: no push, merge, tag, or GitHub release was performed. Disposable CSV and protected-staging resources were isolated from the development volumes and removed after verification.

## Product and data acceptance

- The representative forecasting profile remains active in the local platform: 1,031 accepted orders, 1,347 order items, 737 accepted payments, 679 inventory events, and 19 quarantined records currently visible in the live console. It retains stable product IDs, daily history, weekly/product variation, stochastic noise, three currencies, valid payment/order relationships, duplicate/conflict cases, and quarantine evidence.
- The profile was processed through Kafka ingestion, Bronze, Silver, warehouse publication, quality, and ML workflows. No analytical or forecast rows were inserted directly into serving tables.
- The running dashboard is [http://localhost:23000](http://localhost:23000). Operational interfaces are [Dagster](http://localhost:3002), [Grafana](http://localhost:3001), and [Prometheus](http://localhost:9090).

## ML acceptance

Current serving run: `71d6d44d-bb84-4a99-88a2-d59acf7ac169`.

| Evidence | Verified value |
| --- | --- |
| Training period / rows | 2026-01-01–2026-11-15 / 624 |
| Evaluation period / rows | 2026-11-16–2026-11-22 / 140 |
| Product coverage | 20 technical product IDs |
| Feature construction | lag 1, lag 7, prior trailing mean, weekday, training-fitted product code |
| Leakage prevention | chronological holdout; target day excluded; recursive future steps use prior observations/predictions only |
| Baseline | seasonal naive (`lag_7`) |
| Candidate | fixed `HistGradientBoostingRegressor` |
| Baseline metrics | MAE 1.657143; WAPE 0.320442 |
| Candidate metrics | MAE 15.590441; WAPE 3.014726 |
| Serving strategy | seasonal naive, selected fairly because candidate MAE was not strictly lower |
| Forecast horizon | 2026-11-23–2026-11-29; 140 persisted future rows across 20 products |
| Freshness presentation | model cutoff, holdout, generation timestamp/age, target period, and target-relative-to-today status are visible in the ML screen |

The candidate’s earlier extreme error was reproduced and explained by sparse legacy training data and a one-day spike. The selection rule and evaluation data were not changed to make the candidate lose. The current dashboard visibly distinguishes 60 rows still future relative to 2026-11-26 from 80 target rows whose dates have elapsed.

## Functional acceptance

### Disposable CSV import

Project `titan-csv-accept` used separate ports, database, Kafka, ClickHouse, and object-store resources. The real authenticated API flow verified:

- valid CSV upload: HTTP 202 with import ID;
- import status: `completed`;
- row outcomes: one `published`, one `rejected`;
- valid row persisted in the disposable downstream store;
- supported resume: HTTP 200;
- unauthorized resume: HTTP 403;
- malformed CSV: HTTP 422.

The disposable stack was torn down with its volumes after the run. No development event stream was mutated by this acceptance.

### Protected browser and authorization acceptance

Disposable project `titan-staging-accept-26baf6bc` passed the protected browser harness. The evidence file is retained locally at `artifacts/protected-browser-titan-staging-accept-26baf6bc.json` and contains no raw tokens.

- OIDC login and authenticated session: passed.
- TLS ingress: passed at `https://staging.local:8443/`.
- All eight screens: passed at desktop and mobile widths.
- API proxy boundary: all probes returned HTTP 200.
- Logout: passed.
- Fresh navigation after logout: redirected to OIDC.
- Viewer / Analyst: protected write 403; admin operation 403.
- Operator: protected write 202; admin operation 403.
- Admin: protected write 202; admin route reached the handler and returned 404 for the unknown action.

## Visual and regression acceptance

Fresh final RC captures are in [`docs/assets/v020-final-rc`](../assets/v020-final-rc) with metadata in [`capture-metadata.json`](../assets/v020-final-rc/capture-metadata.json): 24 images covering all eight routes at desktop dark, mobile dark, and desktop light themes. Targeted inspection confirmed forecast timing/freshness, streaming pagination and dispositions, mobile pipeline stage scrolling, and aligned light-theme Data Sources cards.

| Gate | Result |
| --- | --- |
| Frontend unit tests | 11 passed |
| ESLint | passed |
| TypeScript | passed |
| Production build | passed |
| Python tests | 116 passed, 6 skipped |
| Ruff | passed |
| mypy | passed, 135 files |
| Focused auth/security/browser evidence tests | 17 passed, 1 skipped |
| npm production dependency audit | 0 vulnerabilities |
| pip-audit | no known vulnerabilities |
| axe/WCAG and responsive audit | 0 violations, 8 routes × 4 viewports |
| Browser acceptance | protected OIDC/TLS/RBAC run passed |

## Intentional limitations

- GitHub Actions, hosted CI, push, tag, and release publication remain intentionally unrun because the user requested final personal approval before publication.
- Forecasts and demand metrics are synthetic demonstration evidence, not production demand commitments.
- The global time-range selector is explicitly scoped to observed activity charts; snapshot metrics and non-windowed API responses remain current.
- The protected acceptance is disposable local staging evidence, not a claim about an externally hosted deployment.
