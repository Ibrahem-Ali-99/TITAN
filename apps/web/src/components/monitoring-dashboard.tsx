"use client";

import { useEffect, useMemo, useState } from "react";

import {
  DataTable,
  Empty,
  ErrorState,
  Kpi,
  LoadingState,
  PageHeader,
  SectionTitle,
  StatusBadge,
  Td,
  TechnicalDetails,
  Th,
} from "@/components/ui/ops";
import { ApiError, getJson } from "@/lib/api";
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
  const [runs, setRuns] = useState<Run[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [openId, setOpenId] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [next, history] = await Promise.all([
          getJson<Overview>("/api/v1/monitoring/overview"),
          getJson<{ runs: Run[] }>("/api/v1/monitoring/runs?limit=30"),
        ]);
        if (!cancelled) {
          setOverview(next);
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
  return (
    <div>
      <PageHeader
        title="System monitoring"
        description="Pipeline health and measurable operational signals"
      />
      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        {overview.freshness.map((stage) => (
          <Kpi
            key={stage.stage}
            label={`${stage.stage} freshness`}
            value={formatAge(stage.age_seconds)}
            hint={stage.detail}
            footnote={stage.observed_at ? formatUtc(stage.observed_at) + " UTC" : "No timestamp stored"}
          />
        ))}
        <Kpi
          label="Consumer lag"
          value={overview.ingestion_lag.available ? String(overview.ingestion_lag.total_lag) : "Unavailable"}
          hint={overview.ingestion_lag.detail}
          footnote={overview.ingestion_lag.consumer_group}
        />
      </div>
      <div className="mb-4 flex flex-wrap gap-2">
        <StatusBadge tone={overview.ingestion_lag.available ? "info" : "warn"}>
          {overview.ingestion_lag.available ? "Lag measured" : "Lag not measured"}
        </StatusBadge>
        <StatusBadge tone={unresolved.length === 0 ? "ok" : "bad"}>
          {unresolved.length === 0 ? "No unresolved failure in this page" : `${unresolved.length} unresolved in this page`}
        </StatusBadge>
        <StatusBadge tone="neutral">
          Admin {overview.admin_auth === "token" ? "token configured" : "closed"}
        </StatusBadge>
        {overview.model_serving ? <StatusBadge tone="neutral">Forecast {overview.model_serving}</StatusBadge> : null}
      </div>
      <section className="rounded-xl border border-[var(--line)] bg-[var(--surface)] p-4">
        <SectionTitle
          title="Execution history"
          detail="Running rows older than 15 minutes without a finish time are stale. A finished running marker is status unknown. Recovered means a later success exists for the same job in this page."
        />
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
      </section>
      <div className="mt-4 space-y-3">
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
