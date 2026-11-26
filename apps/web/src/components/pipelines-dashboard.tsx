"use client";

import { useEffect, useState } from "react";

import { CopyId, DataTable, Empty, ErrorState, Kpi, LoadingState, Panel, ScopeBar, StatusBadge, StatusPill, Td, TechnicalDetails, Th } from "@/components/ui/ops";
import { ApiError, getJson } from "@/lib/api";
import type { LakeStatus, WarehouseStatus } from "@/lib/types";
import { classifyRun, formatCount, formatDuration, formatUtc, hasLaterSuccess } from "@/lib/utils";

export function PipelinesDashboard() {
  const [status, setStatus] = useState<LakeStatus | null>(null);
  const [warehouse, setWarehouse] = useState<WarehouseStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const next = await getJson<LakeStatus>("/api/v1/lake/status");
        let nextWarehouse: WarehouseStatus | null = null;
        try { nextWarehouse = await getJson<WarehouseStatus>("/api/v1/warehouse/status"); } catch { nextWarehouse = null; }
        if (!cancelled) { setStatus(next); setWarehouse(nextWarehouse); setError(null); }
      } catch (caught) {
        if (!cancelled) setError(caught instanceof ApiError ? caught.message : "The API request failed.");
      } finally { if (!cancelled) setLoading(false); }
    }
    void load();
    const timer = window.setInterval(() => void load(), 15000);
    return () => { cancelled = true; window.clearInterval(timer); };
  }, []);

  if (loading) return <LoadingState label="Loading recorded publications…" />;
  if (!status) return <ErrorState title="Pipelines unavailable" detail={error} />;

  const current = status.current_publication;
  const runs = warehouse?.recent_runs ?? [];
  const durations = runs.map((run) => run.duration_ms).filter((duration): duration is number => duration !== null).sort((a, b) => a - b);
  const median = durations.length ? durations[Math.floor(durations.length / 2)] : null;
  const classifiedRuns = runs.map((run) => ({ run, view: classifyRun({ ...run, recovered: hasLaterSuccess(run, runs) }) }));
  const failed = classifiedRuns.filter(({ view }) => view.label === "Failed").length;
  const running = classifiedRuns.filter(({ view }) => view.label === "Running").length;
  const stale = classifiedRuns.filter(({ view }) => view.label === "Stale").length;
  const succeeded = classifiedRuns.filter(({ view }) => view.label === "Succeeded" || view.label === "Recovered").length;
  const bronze = status.spark_jobs.find((job) => job.job_name.toLowerCase().includes("bronze"));
  const silver = status.spark_jobs.find((job) => job.job_name.toLowerCase().includes("silver"));
  const feature = status.spark_jobs.find((job) => job.job_name.toLowerCase().includes("feature"));
  const stages = [
    { name: "Bronze", state: bronze?.display_state ?? bronze?.state ?? "pending" },
    { name: "Silver", state: silver?.display_state ?? (current ? "ready" : "pending") },
    { name: "Warehouse", state: warehouse?.ready ? "ready" : "pending" },
    { name: "ML features", state: feature?.display_state ?? "not recorded" },
  ];
  const statusTone = (state: string): "ok" | "warn" | "bad" | "info" | "neutral" => {
    const value = state.toLowerCase();
    if (["ready", "succeeded", "success", "healthy"].includes(value)) return "ok";
    if (["failed", "error"].includes(value)) return "bad";
    if (["running", "processing"].includes(value)) return "info";
    if (["pending", "stale"].includes(value)) return "warn";
    return "neutral";
  };

  return (
    <div>
      <div className="page-intro"><div><div className="eyebrow">TITAN / PIPELINES</div><h1>Pipelines</h1><p>Trace processing stages and recorded orchestration evidence</p></div><div className="intro-actions"><span className="updated"><i />15s refresh</span><StatusPill tone={warehouse?.ready ? "ok" : "warn"}>{warehouse?.ready ? "Published" : "Not serving"}</StatusPill></div></div>
      {error ? <p className="prototype-note"><span>⚠ The latest refresh failed. {error}</span></p> : null}
      <div className="kpi-grid">
        <Kpi label="Running executions" value={formatCount(running)} trend="Current page" tone="indigo" footnote="Recorded warehouse runs" />
        <Kpi label="Successful runs" value={formatCount(succeeded)} trend="Recorded" tone="green" footnote="No substitute figures" />
        <Kpi label="Failed runs" value={formatCount(failed)} trend={failed ? "Review" : "Clear"} tone={failed ? "amber" : "green"} footnote={`${stale} stale · attempt history`} />
        <Kpi label="Median duration" value={formatDuration(median)} trend={`${durations.length} samples`} tone="cyan" footnote="Recorded durations" />
      </div>
      <Panel title="Processing stages" detail="Current state of the publication path" action={<StatusPill tone={warehouse?.ready ? "ok" : "warn"}>{warehouse?.ready ? "Ready" : "Pending"}</StatusPill>}>
        <div className="stage-flow" tabIndex={0} aria-label="Processing stages">{stages.map((stage, index) => { const value = stage.state.toLowerCase(); const cls = value.includes("ready") || value.includes("succeed") ? "done" : value.includes("run") ? "running" : ""; return <div className={`stage ${cls}`} key={stage.name}><div>{String(index + 1).padStart(2, "0")}</div>{index < stages.length - 1 ? <span className="stage-line" /> : null}<b>{stage.name}</b><small>{stage.state}</small></div>; })}</div>
      </Panel>
      <div className="two-col lower">
        <Panel title="Execution history" detail="The returned Dagster and warehouse run evidence">
          {runs.length ? <DataTable label="Pipeline execution history"><thead><tr><Th>Started UTC</Th><Th>Job</Th><Th>State</Th><Th>Duration</Th></tr></thead><tbody>{runs.map((run) => { const view = classifyRun({ ...run, recovered: hasLaterSuccess(run, runs) }); return <tr key={run.id}><Td className="font-mono"><b>{formatUtc(run.started_at)}</b></Td><Td>{run.job_name}</Td><Td><StatusBadge tone={view.tone}>{view.label}</StatusBadge></Td><Td>{formatDuration(run.duration_ms)}</Td></tr>; })}</tbody></DataTable> : <Empty>No warehouse or Dagster attempt is in the returned page.</Empty>}
        </Panel>
        <Panel title="Publication lineage" detail="Identifiers are from the active API response" action={<a className="text-btn" href="http://localhost:3002" target="_blank" rel="noreferrer">Open Dagster</a>}>
          <div className="lineage-flow"><div className="lineage-node"><span>01</span><b>Silver publication</b><small>{current ? <CopyId value={current.publication_id} /> : "None"}</small></div><i className="lineage-connector" aria-hidden>→</i><div className="lineage-node"><span>02</span><b>Warehouse</b><small>{warehouse?.warehouse_publication_id ? <CopyId value={warehouse.warehouse_publication_id} /> : "None"}</small></div><i className="lineage-connector" aria-hidden>→</i><div className="lineage-node"><span>03</span><b>Serving</b><small>{warehouse?.ready ? "Validated" : "Not ready"}</small></div></div>
          <div className="freshness"><span>Publication freshness</span><b>{warehouse?.freshness_seconds == null ? "not recorded" : `${warehouse.freshness_seconds}s`}</b><span className="progress" /></div>
        </Panel>
      </div>
      <ScopeBar>Earlier failures stay available without replacing the active publication.</ScopeBar>
      <TechnicalDetails summary="Full attempt history and state detail">{runs.length ? <div className="meta-list">{runs.slice(0, 8).map((run) => <div key={`detail-${run.id}`}><span>{run.job_name}</span><b><StatusPill tone={statusTone(run.state)}>{run.state}</StatusPill> {run.detail ?? run.error_detail ?? "No additional detail"}</b></div>)}</div> : <Empty>No attempt detail is available.</Empty>}</TechnicalDetails>
    </div>
  );
}
