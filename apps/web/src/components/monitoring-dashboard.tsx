"use client";

import { useEffect, useMemo, useState } from "react";

import {
  DataTable,
  Empty,
  ErrorState,
  Kpi,
  LoadingState,
  MetaList,
  PageHeader,
  Panel,
  ScopeBar,
  StatusBadge,
  StatusPill,
  Td,
  TechnicalDetails,
  Th,
} from "@/components/ui/ops";
import { ApiError, getJson } from "@/lib/api";
import type { WarehouseStatus } from "@/lib/types";
import { classifyIncident, matchLatest, platformHealth, summarizeIncidents, type Execution } from "@/lib/incidents";
import { formatAge, formatDuration, formatUtc } from "@/lib/utils";

type Overview = {
  freshness: {
    stage: string;
    observed_at: string | null;
    age_seconds: number | null;
    detail: string;
  }[];
  ingestion_lag: {
    consumer_group: string;
    available: boolean;
    total_lag: number | null;
    detail: string;
  };
  spark_progress: string | null;
  latest_failure: string | null;
  model_serving: string | null;
  admin_auth: string;
};

type Run = {
  id: number;
  job_name: string;
  state: string;
  started_at: string;
  finished_at: string | null;
  duration_ms: number | null;
  detail: string | null;
  error_detail: string | null;
  scope?: string | null;
};

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
          getJson<{ runs: { status: string; warehouse_publication_id: string; finished_at: string | null }[] }>(
            "/api/v1/quality/runs?limit=30",
          ).catch(() => null),
        ]);
        if (!cancelled) {
          setOverview(next);
          setWarehouse(warehouseStatus);
          const matched = warehouseStatus?.warehouse_publication_id
            ? qualityRuns?.runs.find((run) => run.warehouse_publication_id === warehouseStatus.warehouse_publication_id)
            : null;
          setQualityStatus(matched?.status ?? null);
          setRuns(history.runs);
          setJobs(jobStatus ? jobStatus.jobs : null);
          setError(null);
        }
      } catch (caught) {
        if (!cancelled) {
          setError(caught instanceof ApiError ? caught.message : "The API request failed.");
        }
      } finally {
        if (!cancelled) {
          setNow(Date.now());
          setLoading(false);
        }
      }
    }
    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  const summary = useMemo(() => summarizeIncidents(runs, jobs, now), [runs, jobs, now]);

  if (loading) {
    return <LoadingState label="Loading pipeline history…" />;
  }
  if (!overview) {
    return <ErrorState title="Monitoring unavailable" detail={error} />;
  }

  const open = runs.find((run) => run.id === openId) ?? null;
  const warehouseFresh = overview.freshness.find((stage) => stage.stage.toLowerCase().includes("warehouse"));
  const health = platformHealth({
    warehouseReady: warehouse === null ? null : warehouse.ready,
    activeFailures: summary.active.length,
  });
  const stages = [
    {
      stage: "Warehouse publication",
      status: warehouse === null ? "Unknown" : warehouse.ready ? "Succeeded" : "Not ready",
      notes: warehouse === null ? "Status unavailable" : warehouse.ready ? "Ready" : "Not serving",
      tone: warehouse === null ? "neutral" : warehouse.ready ? "ok" : "warn",
      runId: null as number | null,
    },
    ...["quality_gate", "warehouse_models"].map((name) => {
      const latest = jobs?.find((job) => job.job_name === name) ?? null;
      const incident = latest ? classifyIncident(latest, runs, latest) : null;
      const matched = latest ? runs.find((run) => run.id === latest.id) : null;
      return {
        stage: name === "quality_gate" ? "Quality gate" : "dbt models",
        status: incident?.label ?? (jobs === null ? "Unknown" : "No row"),
        notes: matched ? formatDuration(matched.duration_ms) : jobs === null ? "Latest execution was not loaded" : "No execution stored",
        tone: incident?.tone ?? "neutral",
        runId: matched?.id ?? null,
      };
    }),
  ];
  const toneClass = (tone: string) =>
    tone === "ok" ? "text-[var(--ok)]" : tone === "bad" ? "text-[var(--bad)]" : tone === "warn" ? "text-[var(--warn)]" : "text-[var(--muted)]";
  return (
    <div>
      <PageHeader
        title="System monitoring"
        description="Pipeline health and measurable operational signals"
      />
      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi
          label="Kafka lag"
          value={overview.ingestion_lag.available ? String(overview.ingestion_lag.total_lag) : "Unavailable"}
          footnote={overview.ingestion_lag.consumer_group}
        />
        <Kpi
          label="Warehouse"
          value={warehouse?.ready ? "Ready" : warehouse ? "Not ready" : "Unavailable"}
          footnote={warehouse?.ready ? "Published" : warehouseFresh?.observed_at ? formatAge(warehouseFresh.age_seconds) : "No timestamp"}
        />
        <Kpi
          label="Quality"
          value={qualityStatus === "passed" ? "Passed" : qualityStatus ? "Failed" : "No evidence"}
          footnote="Serving publication"
        />
        <Kpi
          label="ML serving"
          value={overview.model_serving?.includes("naive") ? "Naive" : overview.model_serving ?? "Not recorded"}
          footnote={overview.model_serving ?? "Strategy not recorded"}
        />
      </div>
      <div className="grid min-w-0 gap-4 xl:grid-cols-[minmax(0,1.4fr)_minmax(240px,0.7fr)]">
        <Panel title="Recent executions">
          <div className="grid grid-cols-[1.2fr_0.8fr_1fr] gap-3 pb-2 text-[10px] tracking-[0.14em] text-[var(--muted)] uppercase">
            <span>Stage</span>
            <span>Status</span>
            <span>Notes</span>
          </div>
          {stages.map((item) => (
            <button
              key={item.stage}
              type="button"
              className="grid w-full grid-cols-[1.2fr_0.8fr_1fr] gap-3 border-t border-[var(--line)] py-3 text-left text-sm"
              onClick={() => item.runId !== null && setOpenId(openId === item.runId ? null : item.runId)}
            >
              <span>{item.stage}</span>
              <span className={toneClass(item.tone)}>{item.status}</span>
              <span className="text-[var(--muted)]">{item.notes}</span>
            </button>
          ))}
          {summary.active
            .filter((job) => job.job_name !== "quality_gate" && job.job_name !== "warehouse_models")
            .map((job) => (
              <div key={job.job_name} className="grid grid-cols-[1.2fr_0.8fr_1fr] gap-3 border-t border-[var(--line)] py-3 text-sm">
                <span>{job.job_name}</span>
                <span className="text-[var(--bad)]">Failed</span>
                <span className="text-[var(--muted)]">Current execution</span>
              </div>
            ))}
          {summary.recovered > 0 ? (
            <div className="grid grid-cols-[1.2fr_0.8fr_1fr] gap-3 border-t border-[var(--line)] py-3 text-sm">
              <span>Historical failure</span>
              <span className="text-[var(--ok)]">Recovered</span>
              <span className="text-[var(--muted)]">{summary.recovered} confirmed</span>
            </div>
          ) : null}
          {summary.recoveryUnknown > 0 ? (
            <div className="grid grid-cols-[1.2fr_0.8fr_1fr] gap-3 border-t border-[var(--line)] py-3 text-sm">
              <span>Historical failure</span>
              <span className="text-[var(--warn)]">Recovery unknown</span>
              <span className="text-[var(--muted)]">{summary.recoveryUnknown} not active</span>
            </div>
          ) : null}
        </Panel>
        <Panel
          title="Service health"
          detail="Latest observed signals"
          action={<StatusPill tone={health.tone}>{health.label}</StatusPill>}
        >
          <MetaList
            rows={[
              { label: "Kafka consumer", value: overview.ingestion_lag.available ? `${overview.ingestion_lag.total_lag} records lag` : "Not measured" },
              { label: "Spark checkpoint", value: overview.spark_progress ? "Report stored" : "No report" },
              { label: "Warehouse serving", value: warehouse === null ? "Unavailable" : warehouse.ready ? "Verified" : "Not verified" },
              { label: "ML predictions", value: overview.model_serving ?? "Not recorded" },
            ]}
          />
          {summary.recovered > 0 ? (
            <div className="mt-3 rounded-lg border border-[var(--line)] px-3 py-2">
              <p className="text-[10px] tracking-[0.14em] text-[var(--ok)] uppercase">Recovered incident</p>
              <p className="mt-1 text-sm">Previous failure retained</p>
              <p className="text-xs text-[var(--muted)]">A later success of the same job is confirmed. It is not an active incident.</p>
            </div>
          ) : null}
          {summary.recoveryUnknown > 0 ? (
            <div className="mt-3 rounded-lg border border-[var(--line)] px-3 py-2">
              <p className="text-[10px] tracking-[0.14em] text-[var(--warn)] uppercase">Historical failure · Recovery unknown</p>
              <p className="mt-1 text-sm">Not an active incident</p>
              <p className="text-xs text-[var(--muted)]">
                {jobs === null
                  ? "The returned history does not show whether this job succeeded afterward."
                  : "The latest recorded attempt for that job is not a confirmed success."}
              </p>
            </div>
          ) : null}
          {summary.active.length > 0 ? (
            <StatusPill tone="bad">{summary.active.length} active {summary.active.length === 1 ? "failure" : "failures"}</StatusPill>
          ) : null}
        </Panel>
      </div>
      <ScopeBar>Current status, last activity, and historical incidents stay distinct.</ScopeBar>
      <div className="mt-4 space-y-3">
        <TechnicalDetails summary="Full run history">
        {runs.length === 0 ? (
          <Empty>No pipeline attempts are stored.</Empty>
        ) : (
          <DataTable label="Pipeline runs">
            <thead>
              <tr>
                <Th>Started UTC</Th>
                <Th>Job</Th>
                <Th>Reading</Th>
                <Th>Duration</Th>
                <Th>Open</Th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run, index) => {
                const view = summary.incidents[index] ?? classifyIncident(run, runs, matchLatest(run, jobs));
                return (
                  <tr key={run.id}>
                    <Td className="font-mono text-xs">{formatUtc(run.started_at)}</Td>
                    <Td>{run.job_name}</Td>
                    <Td>
                      <StatusBadge tone={view.tone === "ok" ? "ok" : view.tone === "bad" ? "bad" : view.tone === "warn" ? "warn" : "neutral"}>
                        {view.label}
                      </StatusBadge>
                      <span className="ml-2 text-xs text-zinc-500">stored {run.state}</span>
                    </Td>
                    <Td>{formatDuration(run.duration_ms)}</Td>
                    <Td>
                      <button type="button" className="text-xs text-sky-300" onClick={() => setOpenId(openId === run.id ? null : run.id)}>
                        Details
                      </button>
                    </Td>
                  </tr>
                );
              })}
            </tbody>
          </DataTable>
        )}
        </TechnicalDetails>
        <TechnicalDetails summary="Latest stored failure text">
          <p>{overview.latest_failure ?? "No failed pipeline run is stored."}</p>
          <p className="text-xs text-zinc-500">
            This string is the newest failed row. It is an active incident only when that attempt is the latest verified execution of the same job.
          </p>
        </TechnicalDetails>
        <TechnicalDetails summary="Spark checkpoint progress">
          <p className="font-mono text-xs break-all">{overview.spark_progress ?? "No Bronze report is stored."}</p>
          <p className="text-xs text-zinc-500">Checkpoint offsets are not the ingestion consumer lag.</p>
        </TechnicalDetails>
        {open ? (
          <TechnicalDetails summary={`Run ${open.id}`}>
            <p>
              {open.job_name} · finished {formatUtc(open.finished_at)} · {open.detail ?? "no detail"}
            </p>
            <p className="text-xs break-all text-zinc-400">{open.error_detail ?? "No error text."}</p>
          </TechnicalDetails>
        ) : null}
      </div>
    </div>
  );
}
