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
import { classifyRun, formatAge, formatDuration, formatUtc, hasLaterSuccess } from "@/lib/utils";

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
};

export function MonitoringDashboard() {
  const [overview, setOverview] = useState<Overview | null>(null);
  const [warehouse, setWarehouse] = useState<WarehouseStatus | null>(null);
  const [qualityStatus, setQualityStatus] = useState<string | null>(null);
  const [runs, setRuns] = useState<Run[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [openId, setOpenId] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [next, history, warehouseStatus, qualityRuns] = await Promise.all([
          getJson<Overview>("/api/v1/monitoring/overview"),
          getJson<{ runs: Run[] }>("/api/v1/monitoring/runs?limit=30"),
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
          setError(null);
        }
      } catch (caught) {
        if (!cancelled) {
          setError(caught instanceof ApiError ? caught.message : "The API request failed.");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }
    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  const unresolved = useMemo(
    () => runs.filter((run) => run.state === "failed" && !hasLaterSuccess(run, runs)),
    [runs],
  );

  if (loading) {
    return <LoadingState label="Loading pipeline history…" />;
  }
  if (!overview) {
    return <ErrorState title="Monitoring unavailable" detail={error} />;
  }

  const open = runs.find((run) => run.id === openId) ?? null;
  const warehouseFresh = overview.freshness.find((stage) => stage.stage.toLowerCase().includes("warehouse"));
  const recoveredCount = runs.filter((run) => run.state === "failed" && hasLaterSuccess(run, runs)).length;
  const stages = [
    {
      stage: "Warehouse publication",
      status: warehouse?.ready ? "Succeeded" : "Not ready",
      notes: warehouse?.ready ? "Ready" : "Not serving",
      ok: Boolean(warehouse?.ready),
      runId: null as number | null,
    },
    ...["quality_gate", "warehouse_models"].map((name) => {
      const run = runs.find((item) => item.job_name === name && item.state === "succeeded") ?? runs.find((item) => item.job_name === name);
      const view = run ? classifyRun({ ...run, recovered: hasLaterSuccess(run, runs) }) : null;
      return {
        stage: name === "quality_gate" ? "Quality gate" : "dbt models",
        status: view?.label ?? "No row",
        notes: run ? formatDuration(run.duration_ms) : "Not in this page",
        ok: view?.tone === "ok",
        runId: run?.id ?? null,
      };
    }),
  ];
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
              <span className={item.ok ? "text-[var(--ok)]" : "text-[var(--warn)]"}>{item.status}</span>
              <span className="text-[var(--muted)]">{item.notes}</span>
            </button>
          ))}
          {recoveredCount > 0 ? (
            <div className="grid grid-cols-[1.2fr_0.8fr_1fr] gap-3 border-t border-[var(--line)] py-3 text-sm">
              <span>Historical failure</span>
              <span className="text-[var(--muted)]">Recovered</span>
              <span className="text-[var(--muted)]">{recoveredCount} retained</span>
            </div>
          ) : null}
        </Panel>
        <Panel
          title="Service health"
          detail="Latest observed signals"
          action={unresolved.length === 0 && warehouse?.ready && overview.ingestion_lag.available ? <StatusPill tone="ok">Ready</StatusPill> : <StatusPill tone="warn">Partial</StatusPill>}
        >
          <MetaList
            rows={[
              { label: "Kafka consumer", value: overview.ingestion_lag.available ? `${overview.ingestion_lag.total_lag} records lag` : "Not measured" },
              { label: "Spark checkpoint", value: overview.spark_progress ? "Report stored" : "No report" },
              { label: "Warehouse serving", value: warehouse?.ready ? "Verified" : "Not verified" },
              { label: "ML predictions", value: overview.model_serving ?? "Not recorded" },
            ]}
          />
          {recoveredCount > 0 ? (
            <div className="mt-3 rounded-lg border border-[var(--line)] px-3 py-2">
              <p className="text-[10px] tracking-[0.14em] text-[var(--warn)] uppercase">Recovered incident</p>
              <p className="mt-1 text-sm">Previous failure retained</p>
              <p className="text-xs text-[var(--muted)]">Historical. A later success exists for that job. It is not an active publication failure.</p>
            </div>
          ) : null}
          {unresolved.length > 0 ? <StatusPill tone="bad">{unresolved.length} unresolved on this page</StatusPill> : null}
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
              {runs.map((run) => {
                const view = classifyRun({ ...run, recovered: hasLaterSuccess(run, runs) });
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
            This string is the newest failed row. It is a current incident only when that job has no later success in the table above.
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
