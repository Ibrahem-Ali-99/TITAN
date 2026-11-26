"use client";

import { useEffect, useMemo, useState } from "react";

import { DataTable, Empty, ErrorState, Kpi, LoadingState, PageHeader, Panel, ScopeBar, StatusBadge, StatusPill, Td, TechnicalDetails, Th } from "@/components/ui/ops";
import { RunDurationChart } from "@/components/ui/charts";
import { ApiError, getJson } from "@/lib/api";
import type { WarehouseStatus } from "@/lib/types";
import { classifyIncident, matchLatest, platformHealth, summarizeIncidents, type Execution } from "@/lib/incidents";
import { formatAge, formatDuration, formatUtc } from "@/lib/utils";

type Overview = {
  freshness: { stage: string; observed_at: string | null; age_seconds: number | null; detail: string }[];
  ingestion_lag: { consumer_group: string; available: boolean; total_lag: number | null; detail: string };
  spark_progress: string | null;
  latest_failure: string | null;
  model_serving: string | null;
  admin_auth: string;
};
type Run = { id: number; job_name: string; state: string; started_at: string; finished_at: string | null; duration_ms: number | null; detail: string | null; error_detail: string | null; scope?: string | null };

export function MonitoringDashboard() {
  const [overview, setOverview] = useState<Overview | null>(null);
  const [warehouse, setWarehouse] = useState<WarehouseStatus | null>(null);
  const [qualityStatus, setQualityStatus] = useState<string | null>(null);
  const [runs, setRuns] = useState<Run[]>([]);
  const [jobs, setJobs] = useState<Execution[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [openId, setOpenId] = useState<number | null>(null);
  const [now, setNow] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [next, history, jobStatus, warehouseStatus, qualityRuns] = await Promise.all([
          getJson<Overview>("/api/v1/monitoring/overview"),
          getJson<{ runs: Run[] }>("/api/v1/monitoring/runs?limit=30"),
          getJson<{ jobs: Execution[] }>("/api/v1/monitoring/jobs").catch(() => null),
          getJson<WarehouseStatus>("/api/v1/warehouse/status").catch(() => null),
          getJson<{ runs: { status: string; warehouse_publication_id: string; finished_at: string | null }[] }>("/api/v1/quality/runs?limit=30").catch(() => null),
        ]);
        if (!cancelled) {
          setOverview(next);
          setWarehouse(warehouseStatus);
          const matched = warehouseStatus?.warehouse_publication_id ? qualityRuns?.runs.find((run) => run.warehouse_publication_id === warehouseStatus.warehouse_publication_id) : null;
          setQualityStatus(matched?.status ?? null);
          setRuns(history.runs);
          setJobs(jobStatus?.jobs ?? null);
          setError(null);
        }
      } catch (caught) {
        if (!cancelled) setError(caught instanceof ApiError ? caught.message : "The API request failed.");
      } finally {
        if (!cancelled) { setNow(Date.now()); setLoading(false); }
      }
    }
    void load();
    return () => { cancelled = true; };
  }, []);

  const summary = useMemo(() => summarizeIncidents(runs, jobs, now), [runs, jobs, now]);
  if (loading) return <LoadingState label="Loading infrastructure telemetry…" />;
  if (!overview) return <ErrorState title="Monitoring unavailable" detail={error} />;

  const health = platformHealth({ warehouseReady: warehouse === null ? null : warehouse.ready, activeFailures: summary.active.length, latestUnresolved: jobs === null || jobs.some((job) => classifyIncident(job, [], job, now).kind !== "succeeded") });
  const durationPoints = runs.filter((run) => run.duration_ms !== null).slice(0, 8).reverse().map((run, index) => ({ label: `${run.job_name.slice(0, 12)} ${index + 1}`, duration_ms: run.duration_ms ?? 0 }));
  const services = [
    { name: "Kafka consumer", status: overview.ingestion_lag.available ? "Healthy" : "Unavailable", metric: overview.ingestion_lag.available ? `${overview.ingestion_lag.total_lag ?? 0} lag` : "Unavailable", signal: overview.ingestion_lag.available ? "Observed" : "No current signal", tone: overview.ingestion_lag.available ? "ok" : "warn" },
    { name: "Spark checkpoints", status: overview.spark_progress ? "Healthy" : "Unavailable", metric: overview.spark_progress ? "Report stored" : "Unavailable", signal: overview.spark_progress ? "Observed" : "No current signal", tone: overview.spark_progress ? "ok" : "warn" },
    { name: "Warehouse serving", status: warehouse?.ready ? "Healthy" : warehouse ? "Stale" : "Unavailable", metric: warehouse?.ready ? formatAge(warehouse.freshness_seconds) : "Unavailable", signal: warehouse?.published_at ? formatUtc(warehouse.published_at) : "No current signal", tone: warehouse?.ready ? "ok" : "warn" },
    { name: "Quality gate", status: qualityStatus === "passed" ? "Healthy" : qualityStatus ? "Failed" : "Unavailable", metric: qualityStatus ?? "Unavailable", signal: qualityStatus ? "Observed" : "No current signal", tone: qualityStatus === "passed" ? "ok" : "bad" },
    { name: "ML serving", status: overview.model_serving ? "Healthy" : "Unavailable", metric: overview.model_serving ?? "Unavailable", signal: overview.model_serving ? "Observed" : "No current signal", tone: overview.model_serving ? "ok" : "warn" },
    { name: "Admin auth", status: overview.admin_auth === "enabled" ? "Healthy" : "Configured", metric: overview.admin_auth, signal: "Configuration", tone: "ok" },
  ];

  return <div>
    <PageHeader title="Monitoring" description="Infrastructure telemetry, service health, and operational incidents." aside={<StatusPill tone={health.tone}>{health.label}</StatusPill>} />
    <div className="monitor-grid">
      <Kpi label="Readiness checks" value={health.tone === "ok" ? "Current" : "Review"} footnote="Platform evidence" tone="green" />
      <Kpi label="Latest run" value={durationPoints.length ? formatDuration(durationPoints.at(-1)?.duration_ms ?? null) : "—"} footnote="Stored duration" tone="cyan" />
      <Kpi label="Consumer lag" value={overview.ingestion_lag.available ? String(overview.ingestion_lag.total_lag ?? 0) : "Unavailable"} footnote={overview.ingestion_lag.consumer_group} />
      <Kpi label="Active incidents" value={String(summary.active.length)} footnote="Latest verified state" tone="amber" />
    </div>
    <Panel title="Service health matrix" detail="Live infrastructure status and telemetry">
      <div className="health-matrix" tabIndex={0} aria-label="Service health matrix">
        <div className="matrix-head"><span>Service</span><span>Status</span><span>Availability</span><span>Primary metric</span><span>Last signal</span><span /></div>
        {services.map((service) => <div className="health-row" key={service.name}><span className="service-name"><b>{service.name}</b></span><StatusPill tone={service.tone === "ok" ? "ok" : service.tone === "bad" ? "bad" : "warn"}>{service.status}</StatusPill><code>{service.status === "Healthy" ? "current" : "—"}</code><span className={service.metric === "Unavailable" ? "unavailable-text" : ""}>{service.metric}</span><span className={`signal ${service.status.toLowerCase()}`}><i />{service.signal}</span><span /></div>)}
      </div>
    </Panel>
    <div className="two-col">
      <Panel title="Pipeline run duration" detail="Recorded durations; milliseconds are not normalized across jobs">
        {durationPoints.length ? <RunDurationChart points={durationPoints} /> : <Empty>No run telemetry is stored.</Empty>}
      </Panel>
      <Panel title="Operational activity" detail="Run activity is separate from active incident state">
        <div className="timeline">{runs.slice(0, 5).map((run) => { const incident = classifyIncident(run, runs, matchLatest(run, jobs)); const label = incident.tone === "bad" ? "Failed" : incident.tone === "warn" ? "Review" : "Recorded"; return <div key={run.id}><code>{formatUtc(run.started_at).slice(11, 16)}</code><i className={incident.tone} /><span><b>{run.job_name}</b><small>{run.detail ?? run.state}</small></span><StatusPill tone={incident.tone === "bad" ? "bad" : incident.tone === "warn" ? "warn" : "neutral"}>{label}</StatusPill></div>; })}</div>
      </Panel>
    </div>
    <div className="stale-note"><span><b>Missing metrics policy:</b> unavailable or stale telemetry is labeled explicitly and is never represented as zero.</span></div>
    <ScopeBar>Current status, last activity, and historical incidents stay distinct. <a className="text-btn" href="http://localhost:3001" target="_blank" rel="noreferrer">Open Grafana</a> · <a className="text-btn" href="http://localhost:9090" target="_blank" rel="noreferrer">Open Prometheus</a></ScopeBar>
    <TechnicalDetails summary="Full monitoring history">
      {runs.length === 0 ? <Empty>No pipeline attempts are stored.</Empty> : <DataTable label="Pipeline runs"><thead><tr><Th>Started UTC</Th><Th>Job</Th><Th>Reading</Th><Th>Duration</Th><Th>Open</Th></tr></thead><tbody>{runs.map((run, index) => { const incident = summary.incidents[index] ?? classifyIncident(run, runs, matchLatest(run, jobs)); return <tr key={run.id}><Td className="font-mono text-xs">{formatUtc(run.started_at)}</Td><Td>{run.job_name}</Td><Td><StatusBadge tone={incident.tone === "ok" ? "ok" : incident.tone === "bad" ? "bad" : incident.tone === "warn" ? "warn" : "neutral"}>{incident.label}</StatusBadge><span className="ml-2 text-xs text-zinc-400">stored {run.state}</span></Td><Td>{formatDuration(run.duration_ms)}</Td><Td><button type="button" className="text-xs text-sky-300" onClick={() => setOpenId(openId === run.id ? null : run.id)}>Details</button></Td></tr>; })}</tbody></DataTable>}
    </TechnicalDetails>
    {openId !== null ? <TechnicalDetails summary={`Run ${openId}`}><p>{runs.find((run) => run.id === openId)?.detail ?? "No detail stored."}</p></TechnicalDetails> : null}
  </div>;
}
