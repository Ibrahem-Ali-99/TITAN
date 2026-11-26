"use client";

import { useEffect, useMemo, useState } from "react";

import { ActivityChart } from "@/components/ui/charts";
import {
  CopyId,
  DataTable,
  Empty,
  ErrorState,
  Kpi,
  LoadingState,
  Notice,
  PageHeader,
  Panel,
  ScopeBar,
  StatusPill,
  Td,
  TechnicalDetails,
  Th,
} from "@/components/ui/ops";
import { ApiError, getJson } from "@/lib/api";
import type { HealthReady, IngestionStats, LakeStatus, RecentEvent, RecentEventsResponse, WarehouseStatus } from "@/lib/types";
import { formatAge, formatCount, formatMoney, formatUtc } from "@/lib/utils";

const REFRESH_MS = 5000;

type QualityRun = {
  run_id: string;
  status: string;
  warehouse_publication_id: string;
  failure_count: number;
  injected: string | null;
  finished_at: string | null;
};

type Snapshot = {
  ready: HealthReady;
  stats: IngestionStats;
  recent: RecentEventsResponse;
  lake: LakeStatus | null;
  warehouse: WarehouseStatus | null;
  warehouseError: string | null;
  quality: QualityRun | null;
  lag: { available: boolean; total: number | null; detail: string } | null;
  serving: string | null;
  fetchedAt: string;
};

export function OverviewDashboard() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [initialError, setInitialError] = useState<string | null>(null);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [sort, setSort] = useState<"ingested" | "disposition">("ingested");

  useEffect(() => {
    let cancelled = false;
    async function load(isInitial: boolean) {
      try {
        const overviewResults = await Promise.allSettled([
          getJson<HealthReady>("/api/health/ready"),
          getJson<IngestionStats>("/api/v1/ingestion/stats"),
          getJson<RecentEventsResponse>("/api/v1/events/recent?limit=20"),
        ] as const);
        const readyResult = overviewResults[0];
        const statsResult = overviewResults[1];
        const recentResult = overviewResults[2];
        if (readyResult.status !== "fulfilled" || statsResult.status !== "fulfilled") {
          throw new ApiError("The readiness or ingestion response was unavailable.", 503);
        }
        const ready = readyResult.value;
        const stats = statsResult.value;
        const recent = recentResult.status === "fulfilled" ? recentResult.value : { items: [], limit: 20, returned: 0 };
        let warehouse: WarehouseStatus | null = null;
        let lake: LakeStatus | null = null;
        let warehouseError: string | null = null;
        let quality: QualityRun | null = null;
        let lag: Snapshot["lag"] = null;
        let serving: string | null = null;
        try {
          const monitor = await getJson<{
            ingestion_lag: { available: boolean; total_lag: number | null; detail: string };
            model_serving: string | null;
          }>("/api/v1/monitoring/overview");
          lag = {
            available: monitor.ingestion_lag.available,
            total: monitor.ingestion_lag.total_lag,
            detail: monitor.ingestion_lag.detail,
          };
          serving = monitor.model_serving;
        } catch {
          lag = null;
        }
        try {
          lake = await getJson<LakeStatus>("/api/v1/lake/status");
        } catch {
          lake = null;
        }
        try {
          warehouse = await getJson<WarehouseStatus>("/api/v1/warehouse/status");
        } catch (error) {
          warehouseError = error instanceof ApiError ? error.message : "Warehouse status failed.";
        }
        try {
          const runs = await getJson<{ runs: QualityRun[] }>("/api/v1/quality/runs?limit=30");
          const publication = warehouse?.warehouse_publication_id;
          quality =
            runs.runs.find((run) => publication && run.warehouse_publication_id === publication) ??
            null;
        } catch {
          quality = null;
        }
        if (cancelled) {
          return;
        }
        setSnapshot({ ready, stats, recent, lake, warehouse, warehouseError, quality, lag, serving, fetchedAt: new Date().toISOString() });
        setInitialError(null);
        setRefreshError(null);
      } catch (error) {
        if (cancelled) {
          return;
        }
        const message = error instanceof ApiError ? error.message : "The API request failed.";
        if (isInitial) {
          setInitialError(message);
        } else {
          setRefreshError(message);
        }
      } finally {
        if (!cancelled && isInitial) {
          setLoading(false);
        }
      }
    }
    void load(true);
    const timer = window.setInterval(() => void load(false), REFRESH_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  const events = useMemo(() => {
    const items = [...(snapshot?.recent.items ?? [])];
    items.sort((left, right) => {
      if (sort === "disposition") {
        return left.disposition.localeCompare(right.disposition);
      }
      return right.ingested_at.localeCompare(left.ingested_at);
    });
    return items;
  }, [snapshot, sort]);

  if (loading) {
    return <LoadingState label="Loading persisted ingestion state…" />;
  }
  if (!snapshot) {
    return <ErrorState title="Overview unavailable" detail={initialError} />;
  }

  const { stats, warehouse, quality, lag, serving } = snapshot;
  const activity = stats.recent_activity.map((point) => ({
    label: formatUtc(point.minute).slice(11, 16),
    persisted: point.persisted_events,
  }));
  const current = snapshot.lake?.current_publication;
  const latestRuns = warehouse?.recent_runs ?? [];
  const serviceState = (name: string) => snapshot.ready.checks[name] ?? "unavailable";
  const pipelineNode = (label: string, detail: string, state: string) => ({ label, detail, state });
  const nodes = [
    pipelineNode("Kafka", "Event transport", snapshot.lag?.available ? "observed" : "not measured"),
    pipelineNode("PostgreSQL", "Persisted evidence", serviceState("postgres")),
    pipelineNode("MinIO", "Object storage", snapshot.lake ? "configured" : "not measured"),
    pipelineNode("Spark Bronze", "Raw event layer", snapshot.lake?.spark_jobs.find((job) => job.job_name.toLowerCase().includes("bronze"))?.display_state ?? "unavailable"),
    pipelineNode("Spark Silver", "Curated commerce", current ? "ready" : "pending"),
    pipelineNode("ClickHouse", "Published warehouse", warehouse?.ready ? "ready" : "pending"),
    pipelineNode("dbt", "Serving models", latestRuns.some((run) => run.job_name === "warehouse_models" && run.state === "succeeded") ? "ready" : "recorded"),
  ];
  const toneFor = (state: string): "ok" | "warn" | "info" | "neutral" => {
    const normalized = state.toLowerCase();
    if (["ok", "ready", "succeeded", "running", "healthy"].includes(normalized)) return "ok";
    if (["warn", "pending", "stale"].includes(normalized)) return "warn";
    if (["error", "failed", "unavailable"].includes(normalized)) return "warn";
    return "neutral";
  };
  return (
    <div>
      <PageHeader
        title="Overview"
        description="Real-time intelligence across your data infrastructure"
        aside={
          <>
            <span className="updated"><i />Updated {formatAge(0)}</span>
            <StatusPill tone={warehouse?.ready ? "ok" : "warn"}>{warehouse?.ready ? "Live data connected" : "Warehouse not ready"}</StatusPill>
          </>
        }
      />
      {refreshError ? <Notice>The latest refresh failed. Figures below are the previous response. {refreshError}</Notice> : null}
      <div className="kpi-grid">
        <Kpi label="Accepted orders" value={formatCount(warehouse?.order_count)} trend={warehouse?.ready ? "Published" : "Unavailable"} tone="green" footnote="Published warehouse snapshot" />
        <Kpi label="Payments processed" value={formatCount(stats.accepted_payments)} trend="Recorded" tone="cyan" footnote="Accepted ingestion events" />
        <Kpi label="Inventory updates" value={formatCount(stats.inventory_updates)} trend="Recorded" tone="green" footnote="Accepted inventory events" />
        <Kpi label="Warehouse state" value={warehouse?.ready ? "Ready" : "—"} trend={warehouse?.ready ? "Published snapshot" : "Awaiting publication"} tone={warehouse?.ready ? "green" : "amber"} footnote={`${Object.values(snapshot.ready.checks).filter((state) => state === "ok").length}/${Object.keys(snapshot.ready.checks).length || 0} readiness checks`} />
      </div>
      <div className="overview-grid">
        <Panel className="span-8" title="Event throughput" detail="Persisted events · observed minute buckets" action={<StatusPill tone="info">UTC · recorded</StatusPill>}>
          {activity.length ? <ActivityChart points={activity} /> : <div className="chart-empty" role="status"><strong>No persisted events in this window</strong><span>Recent event records remain available below.</span></div>}
        </Panel>
        <Panel className="span-4" title="Revenue by currency" detail="Published snapshot totals">
          <div className="revenue-list">
            {warehouse?.ready && Object.entries(warehouse.accepted_revenue).length ? Object.entries(warehouse.accepted_revenue).map(([currency, amount], index) => (
              <div className="revenue-row" key={currency}>
                <span className={`currency c${index % 3}`}>{currency}</span>
                <div><b>{formatMoney(amount, currency)}</b><span>Accepted revenue</span></div>
              </div>
            )) : <Empty>{snapshot.warehouseError ?? warehouse?.reason ?? "No published revenue is available."}</Empty>}
          </div>
          <div className="callout">Amounts are snapshot values; no live FX conversion is applied.</div>
        </Panel>
        <Panel className="span-8" title="Data pipeline topology" detail="Evidence path from transport to serving">
          <div className="topology" tabIndex={0} aria-label="Data pipeline topology">
            {nodes.map((node, index) => <div className="topology-step" key={node.label}><div className="node"><span>{String(index + 1).padStart(2, "0")}</span><b>{node.label}</b><small><i className={node.state === "not measured" || node.state === "unavailable" ? "warning" : ""} />{node.state}</small></div>{index < nodes.length - 1 ? <div className="connector"><i /></div> : null}</div>)}
          </div>
          <div className="pipeline-note"><b>Publication</b><span><i className="ok-dot" />{warehouse?.warehouse_publication_id ? warehouse.warehouse_publication_id.slice(0, 18) : "No active warehouse publication"}</span><span>Silver {current?.silver_order_count == null ? "—" : formatCount(current.silver_order_count)} orders</span></div>
        </Panel>
        <Panel className="span-4" title="Dagster orchestration" detail="Latest recorded runs">
          <div className="dagster-score"><div><b>{formatCount(latestRuns.length)}</b><span>recorded runs</span></div><StatusPill tone={latestRuns.some((run) => run.state === "failed") ? "warn" : "ok"}>{latestRuns.some((run) => run.state === "failed") ? "Review" : "Healthy"}</StatusPill></div>
          {latestRuns.slice(0, 4).map((run) => <div className="compact-row" key={run.id}><span><i className="ok-dot" />{run.job_name}</span><b>{run.state}</b></div>)}
          {!latestRuns.length ? <Empty>No recorded orchestration runs.</Empty> : null}
        </Panel>
        <Panel className="span-5" title="Platform health" detail="Live readiness checks">
          <div className="service-list">{Object.entries(snapshot.ready.checks).map(([name, state]) => <div className="service-row" key={name}><span><i className={state === "ok" ? "" : "warning"} />{name}</span><em>{state}</em><span>{state === "ok" ? "Healthy" : "Needs review"}</span><span>›</span></div>)}</div>
        </Panel>
        <Panel className="span-4" title="Recent pipeline activity" detail="Warehouse and quality evidence">
          {latestRuns.slice(0, 4).map((run) => <div className="activity-row" key={run.id}><span className={`activity-icon ${run.state === "failed" ? "failed" : run.state === "running" ? "running" : ""}`}>↗</span><span><b>{run.job_name}</b><small>{formatUtc(run.started_at)}</small></span><StatusPill tone={toneFor(run.state)}>{run.state}</StatusPill></div>)}
          {!latestRuns.length ? <Empty>No pipeline activity recorded.</Empty> : null}
        </Panel>
        <Panel className="span-3" title="Quality & publication" detail="Serving evidence">
          <div className="quality-ring"><div><b>{quality?.status === "passed" ? "100%" : quality ? "—" : "N/A"}</b><span>quality</span></div></div>
          <div className="quality-stats"><span><b>{formatCount(stats.quarantined_records)}</b>quarantined</span><span><b>{formatCount(stats.logical_orders)}</b>logical orders</span></div>
          <div className="publication-brief"><span>ACTIVE PUBLICATION</span><b>{warehouse?.warehouse_publication_id ? warehouse.warehouse_publication_id.slice(0, 14) : "Not serving"}</b><small>{serving ?? "Forecast not recorded"}</small></div>
        </Panel>
      </div>
      <section className="panel card" style={{ marginTop: 10 }}>
        <div className="panel-head"><div><h3>Recent persisted events</h3><p>Sorted within the returned page.</p></div><label className="table-tools"><select aria-label="Sort recent events" value={sort} onChange={(event) => setSort(event.target.value as "ingested" | "disposition")}><option value="ingested">Ingested time</option><option value="disposition">Disposition</option></select></label></div>
        {events.length ? <DataTable label="Recent persisted events"><thead><tr><Th>Ingested UTC</Th><Th>Order</Th><Th>Disposition</Th><Th>Amount</Th><Th>Offset</Th></tr></thead><tbody>{events.map((event) => <EventRow key={event.event_id} event={event} />)}</tbody></DataTable> : <Empty>No events have been persisted yet.</Empty>}
      </section>
      <ScopeBar>Scope: persisted events and published snapshots are distinct. Logical orders {formatCount(stats.logical_orders)}. Lag {lag?.available ? formatCount(lag.total) : "unavailable"}.</ScopeBar>
      <TechnicalDetails summary="Publication identifiers"><p>Warehouse <CopyId value={warehouse?.warehouse_publication_id ?? null} /></p><p>Silver <CopyId value={warehouse?.silver_publication_id ?? null} /></p><p>Quarantined records {formatCount(stats.quarantined_records)}. {stats.metric_definitions.quarantined_records}</p></TechnicalDetails>
    </div>
  );
}

function EventRow({ event }: { event: RecentEvent }) {
  return (
    <tr>
      <Td className="font-mono text-xs">{formatUtc(event.ingested_at)}</Td>
      <Td className="font-mono text-xs">{event.order_id ?? "—"}</Td>
      <Td>{event.disposition}</Td>
      <Td className="font-mono text-xs">
        {event.currency && event.total_amount ? formatMoney(event.total_amount, event.currency) : "—"}
      </Td>
      <Td className="font-mono text-xs text-zinc-400">
        {event.kafka_partition}:{event.kafka_offset}
      </Td>
    </tr>
  );
}
