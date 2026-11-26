# TITAN v0.2.0 UI/UX remediation inventory

This inventory records the acceptance work completed against the authoritative Figma Make archive in `D:\Downloads\TITAN Data Platform.zip`, the current TITAN routes, and the live API responses. No fabricated business values were added.

## Shared design system

- Consolidated page title, panel, KPI, metric-strip, status, table, tab, source-card, lineage, and empty/loading/error patterns in the exported visual system.
- Increased the practical type scale and row spacing for desktop and responsive layouts; retained compact mobile breakpoints.
- Removed the legacy sky-blue action treatment from the shared `Button` component and routed actions through the TITAN visual tokens.
- Added bounded chart bars, readable chart labels, keyboard-visible focus, and explicit unavailable/not-measured states.

## Eight-screen completion inventory

### Overview

- Four real KPI cards remain, with synthetic sparkline values removed.
- Event throughput uses observed minute buckets and bounded bars; sparse activity remains visibly sparse.
- Topology now includes Kafka, PostgreSQL, MinIO, Spark Bronze, Spark Silver, ClickHouse, and dbt. Kafka/MinIO are marked not measured/configured when the API does not expose a direct health check.
- Revenue remains separated by EUR, GBP, and USD with a no-FX-conversion note.

### Streaming

- Replaced quarantine-first hierarchy with metric strip, throughput, recent event distribution, and a prominent recent persisted-event table.
- Added real search and event-type filtering; selecting a row exposes the stored event payload fields.
- Kept quarantine and replay evidence secondary and labeled consumer lag unavailable because the API does not measure it.

### Data Sources

- Rebuilt the source-card row for REST, CSV, and the deterministic synthetic producer using live counts.
- Added CSV upload, REST ingestion contract, and import history tabs.
- Added a professional dropzone-style file control while preserving the live 500-row/1 MiB validation boundary, protected upload/resume behavior, and row-error inspection.

### Pipelines

- Running count now excludes stale runs; stale is a separate, visible state.
- Reworked publication lineage into non-overlapping nodes with copyable real identifiers and a Dagster link.
- Stage flow and execution history distinguish succeeded, failed, recovered, running, stale, and unknown states from the shared classifier.

### Warehouse

- Replaced competing KPI rows with one compact metric strip covering orders, items, payments, inventory, and currency-separated revenue.
- Kept chronological event-time analytics, currency/product selectors, active publication, source identifiers, and freshness evidence visible.

### Data Quality

- Added an explicit current publication quality hero with pass/fail, current failures, historical failures, finding groups, and blocking state.
- Kept historical findings separate from the serving publication and made execution history visible without hiding the primary experience behind a disclosure.
- Retained real severity, injection, publication, and reconciliation evidence; no unsupported score or threshold was introduced.

### Monitoring

- Replaced raw duration text bars with a labeled observed-duration visualization in milliseconds.
- Changed the top summary from a synthetic availability percentage to readiness/current-state semantics.
- Separated operational activity from incidents and linked to local Grafana and Prometheus interfaces.

### Machine Learning

- Preserved the model banner, serving metrics, actual-vs-predicted evaluation chart, comparison, product selector, evaluation results, model history, and anomaly table.
- Removed fabricated KPI sparklines and clarified MAE/WAPE semantics, serving status, and technical product IDs where no authoritative name dimension exists.

## Evidence

- Reference captures and current route captures: [docs/assets/v020-acceptance](/D:/TITAN/docs/assets/v020-acceptance)
- Live browser route: [http://localhost:23000](http://localhost:23000)
- Dagster: [http://localhost:3002](http://localhost:3002)
- Grafana: [http://localhost:3001](http://localhost:3001)
- Prometheus: [http://localhost:9090](http://localhost:9090)
