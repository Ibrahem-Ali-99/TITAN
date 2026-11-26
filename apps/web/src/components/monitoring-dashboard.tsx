"use client";

import { useEffect, useMemo, useState } from "react";
import { ExternalLink, Info } from "lucide-react";

import { DataTable, Empty, ErrorState, Kpi, LoadingState, PageHeader, Panel, StatusBadge, StatusPill, Td, TechnicalDetails, Th } from "@/components/ui/ops";
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
  const durationPoints = runs.filter((run) => run.duration_ms !== null).slice(0, 8).reverse().map((run, index) => ({ label: `${shortJobName(run.job_name)} ${index + 1}`, job_name: run.job_name, run_id: run.id, duration_ms: run.duration_ms ?? 0 }));
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
    <div className="two-col monitoring-panels">
      <Panel title="Pipeline run duration" detail="Recent recorded runs · hover for exact values">
        {durationPoints.length ? <RunDurationChart points={durationPoints} /> : <Empty>No run telemetry is stored.</Empty>}
      </Panel>
      <Panel title="Operational activity" detail="Recent runs · incidents are separate">
        <div className="timeline">{runs.slice(0, 5).map((run) => { const incident = classifyIncident(run, runs, matchLatest(run, jobs)); const label = incident.tone === "bad" ? "Failed" : incident.tone === "warn" ? "Review" : "Recorded"; const timestamp = formatUtc(run.started_at); return <div key={run.id}><time dateTime={run.started_at} title={timestamp}>{formatActivityTime(run.started_at)}</time><i className={`timeline-dot ${incident.tone}`} aria-hidden /><span className="timeline-event"><b>{shortJobName(run.job_name)}</b><small>{activitySummary(run)}</small></span><StatusPill tone={incident.tone === "bad" ? "bad" : incident.tone === "warn" ? "warn" : "neutral"}>{label}</StatusPill></div>; })}</div>
      </Panel>
    </div>
    <div className="monitoring-context">
      <div className="monitoring-policy"><Info size={17} aria-hidden /><div><b>How to read monitoring</b><span>Health is current, activity is recorded, and incidents are historical. Missing or stale metrics stay labeled unavailable rather than becoming zero.</span></div></div>
      <div className="monitoring-links" aria-label="Operational observability tools">
        <a className="monitoring-link" href="http://localhost:3001" target="_blank" rel="noreferrer"><span><b>Grafana</b><small>Dashboards</small></span><ExternalLink size={15} aria-hidden /></a>
        <a className="monitoring-link" href="http://localhost:9090" target="_blank" rel="noreferrer"><span><b>Prometheus</b><small>Metrics explorer</small></span><ExternalLink size={15} aria-hidden /></a>
      </div>
    </div>
    <div className="monitoring-history">
      <TechnicalDetails summary={`Full monitoring history · ${runs.length} recorded runs`}>
        {runs.length === 0 ? <Empty>No pipeline attempts are stored.</Empty> : <DataTable label="Pipeline runs"><thead><tr><Th>Started UTC</Th><Th>Job</Th><Th>Reading</Th><Th>Duration</Th><Th>Open</Th></tr></thead><tbody>{runs.map((run, index) => { const incident = summary.incidents[index] ?? classifyIncident(run, runs, matchLatest(run, jobs)); return <tr key={run.id}><Td className="font-mono text-xs">{formatUtc(run.started_at)}</Td><Td>{run.job_name}</Td><Td><StatusBadge tone={incident.tone === "ok" ? "ok" : incident.tone === "bad" ? "bad" : incident.tone === "warn" ? "warn" : "neutral"}>{incident.label}</StatusBadge><span className="ml-2 text-xs text-zinc-400">stored {run.state}</span></Td><Td>{formatDuration(run.duration_ms)}</Td><Td><button type="button" className="text-xs text-sky-300" onClick={() => setOpenId(openId === run.id ? null : run.id)}>Details</button></Td></tr>; })}</tbody></DataTable>}
      </TechnicalDetails>
    </div>
    {openId !== null ? <TechnicalDetails summary={`Run ${openId}`}><p>{runs.find((run) => run.id === openId)?.detail ?? "No detail stored."}</p></TechnicalDetails> : null}
  </div>;
}

function shortJobName(value: string) {
  if (value === "warehouse_load") return "Warehouse";
  if (value === "quality_gate") return "Quality";
  if (value === "warehouse_models") return "Models";
  if (value === "bronze_stream") return "Bronze";
  return value.slice(0, 10);
}

function activitySummary(run: Run) {
  if (run.job_name === "warehouse_load") return "Warehouse publication";
  if (run.job_name === "quality_gate") return "Quality gate";
  if (run.job_name === "warehouse_models") return "Serving models";
  if (run.job_name === "bronze_stream") return "Bronze checkpoint";
  return run.state;
}

function formatActivityTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-GB", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "UTC" }).format(date) + " UTC";
}
