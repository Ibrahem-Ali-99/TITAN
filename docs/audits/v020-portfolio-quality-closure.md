# TITAN v0.2.0 portfolio-quality closure

Verification date: 2026-11-26 (Africa/Cairo)  
Repository: `D:\TITAN`  
Branch: `feature/titan-v0.2.0-final-ui`

This is the evidence record for the final targeted visual and functional remediation pass. No push, merge, tag, or release was performed.

## Requirement-to-evidence matrix

| Requirement | Implementation | Verification | Status / limitation |
| --- | --- | --- | --- |
| Correct light-theme evidence | `apps/web/src/components/shell.tsx`, `apps/web/src/app/globals.css` persist the theme and apply the same palette through `data-theme` and the app root. | `scripts/audit_a11y.py` and `docs/assets/v020-visual-polish/capture-metadata.json` compare computed dark/light background, surface, text, and border colors on all routes; refresh and cross-route persistence passed. | Complete. |
| Data Quality scan-first presentation | `apps/web/src/components/quality-dashboard.tsx` summarizes findings, separates current from historical counts, and keeps observed/expected/raw identifiers behind Details. The table has an explicit responsive layout. | Desktop and mobile screenshots reviewed; current run showed 9 rows, 0 current failures, 0 blocking rules, structured currency evidence, and visible Details controls. | Complete. |
| Streaming distribution readability | `apps/web/src/lib/streaming.ts`, `streaming-dashboard.tsx`, and the regression test provide proportional non-zero fills and correct zero behavior. | `npm test` includes 4 distribution-bar assertions; browser capture verified visible proportional bars and populated event types. | Complete. |
| Fresh throughput evidence | `scripts/demo_traffic.py` publishes unique order/payment/inventory cycles through the authenticated ingestion API. | Run token `4c4df419d2`: 24 events published, persisted total 2,584, observed minute buckets 5 / 13 / 6. Overview and Streaming screenshots show those live buckets and IDs. | Complete for local synthetic demonstration data. |
| Data Sources and operational copy | Source-card alignment and helper copy were refined without changing upload/import behavior. | Route capture plus existing disposable CSV evidence: upload 202, completed status, 1 published + 1 rejected row, resume 200, unauthorized resume 403, malformed CSV 422. | Complete; disposable stack was removed. |
| All eight routes and responsive layout | Shared components and targeted CSS preserve desktop/mobile behavior. | `scripts/audit_a11y.py`: 0 violations across 8 routes × 4 viewports; capture archive: 24 screenshots (8 routes × dark desktop/mobile/light desktop). | Complete. |
| Accessibility and interaction regression | Keyboard-visible focus, route headings, tables, filters, pagination, and chart presence are checked by the audit/capture scripts. | Final audit exit 0; axe/WCAG violations 0; capture script exit 0. | Complete for the tested local runtime. |
| Protected browser/RBAC evidence | Existing disposable evidence is retained in `artifacts/protected-browser-titan-staging-accept-26baf6bc.json` and role files. | Same-day prior run passed OIDC login/logout, session enforcement, TLS, proxy, all eight routes, and viewer/analyst/operator/admin boundary checks. A fresh rerun with project `titan-staging-portfolio-accept` reached API/web health but stopped at an unhealthy disposable ingress before browser execution and cleaned up. | Prior evidence remains valid for unchanged auth code; fresh ingress rerun is an infrastructure limitation, not claimed as a new pass. |

## Dataset and live runtime evidence

The local platform remains running at [http://localhost:23000](http://localhost:23000) with the representative synthetic commerce dataset. Current serving counts are 1,031 accepted orders, 1,347 order items, 737 accepted payments, 679 inventory events, and 19 quarantined records. The portfolio traffic run added only unique events through the real ingestion boundary; no serving-table rows were fabricated or directly inserted.

Operational interfaces remain [Dagster](http://localhost:3002), [Grafana](http://localhost:3001), and [Prometheus](http://localhost:9090).

## Validation results

- Frontend: 12 tests passed; ESLint passed; TypeScript passed; production build passed.
- Python: 116 tests passed, 6 skipped; Ruff lint passed; mypy passed for 137 source files; secret scan passed; supply-chain check passed.
- Dependencies: `npm audit --audit-level=high` reported 0 vulnerabilities; frozen `pip-audit==2.10.1` reported no known vulnerabilities.
- Browser: final eight-route accessibility/responsive/theme audit passed with 0 violations; final screenshot capture passed with 24 images.
- The full repository `ruff format --check` still identifies two pre-existing generator formatting differences outside this targeted change; the three new/edited audit/demo scripts were formatted and pass Ruff lint and mypy.

## Evidence archive

- Screenshots and computed-style metadata: [`docs/assets/v020-visual-polish`](../assets/v020-visual-polish)
- Packaged archive: [`docs/assets/v020-visual-polish.zip`](../assets/v020-visual-polish.zip)
- Capture automation: [`scripts/capture_portfolio_evidence.py`](../../scripts/capture_portfolio_evidence.py)
- Live traffic automation: [`scripts/demo_traffic.py`](../../scripts/demo_traffic.py)
- Accessibility/theme audit: [`scripts/audit_a11y.py`](../../scripts/audit_a11y.py)

Intentional limitations remain unchanged: the data is synthetic demonstration evidence, the global range selector scopes observed activity charts only, and no hosted publication was attempted.
