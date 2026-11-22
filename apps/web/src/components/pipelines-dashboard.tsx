"use client";

import { useEffect, useState } from "react";

import {
  CopyId,
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
        let runs: WarehouseStatus | null = null;
        try {
          runs = await getJson<WarehouseStatus>("/api/v1/warehouse/status");
        } catch {
          runs = null;
        }
        if (!cancelled) {
          setStatus(next);
          setWarehouse(runs);
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
    const timer = window.setInterval(() => void load(), 15000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  if (loading) {
    return <LoadingState label="Loading recorded publications…" />;
  }
  if (!status) {
    return <ErrorState title="Pipelines unavailable" detail={error} />;
  }

  const current = status.current_publication;
  const runs = warehouse?.recent_runs ?? [];
  const latest = (name: string) => runs.find((run) => run.job_name === name);
  const bronze = status.spark_jobs.find((job) => job.job_name.toLowerCase().includes("bronze"));
  const dbt = latest("warehouse_models");
  const gate = latest("quality_gate");
  const dbtView = dbt ? classifyRun({ ...dbt, recovered: hasLaterSuccess(dbt, runs) }) : null;
  const gateView = gate ? classifyRun({ ...gate, recovered: hasLaterSuccess(gate, runs) }) : null;
  const graph = [
    { id: "01", name: "Kafka", detail: "Event transport", tone: "info" as const },
    { id: "02", name: "Bronze", detail: "Raw event evidence", tone: bronze ? "info" as const : "neutral" as const },
    { id: "03", name: "Silver", detail: "Accepted commerce", tone: current ? "ok" as const : "warn" as const },
    { id: "04", name: "ClickHouse", detail: "Versioned warehouse", tone: warehouse?.ready ? "ok" as const : "warn" as const },
    { id: "05", name: "dbt", detail: "Serving models", tone: dbtView?.tone === "ok" ? "ok" as const : "neutral" as const },
    { id: "06", name: "Quality gate", detail: "Blocking tests", tone: gateView?.tone === "ok" ? "ok" as const : "neutral" as const },
    { id: "07", name: "Published", detail: "Validated snapshot", tone: warehouse?.ready ? "ok" as const : "warn" as const },
  ];
  const load = latest("warehouse_load");
  const loadView = load ? classifyRun({ ...load, recovered: hasLaterSuccess(load, runs) }) : null;
  const linked = Boolean(current && warehouse?.ready && warehouse.warehouse_publication_id);

  return (
    <div>
      <PageHeader
        title="Pipeline orchestration"
        description="Versioned materializations and recovery history"
        aside={warehouse?.ready ? <StatusPill tone="ok">Published</StatusPill> : <StatusPill tone="warn">Not serving</StatusPill>}
      />
      {error ? <p className="mb-3 text-sm text-amber-100">The latest refresh failed. {error}</p> : null}
      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi label="Accepted orders" value={formatCount(current?.silver_order_count)} footnote="Silver" />
        <Kpi label="Pipeline health" value={warehouse?.ready ? "Ready" : "Not ready"} footnote={warehouse ? "Last verified state" : "Status was not returned"} />
        <Kpi label="dbt quality" value={gateView?.tone === "ok" ? "Passed" : gateView?.label ?? "No evidence"} footnote="Published candidate" />
        <Kpi label="Lineage" value={linked ? "Complete" : "Incomplete"} footnote="Silver and warehouse" />
      </div>
      <Panel
        title="Data pipeline"
        detail="Execution and publication graph"
        action={warehouse?.ready ? <StatusPill tone="ok">Published</StatusPill> : undefined}
      >
        <ol className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
          {graph.slice(0, 4).map((stage) => (
            <StageCard key={stage.id} stage={stage} />
          ))}
        </ol>
        <ol className="mt-2 grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
          {graph.slice(4).map((stage) => (
            <StageCard key={stage.id} stage={stage} />
          ))}
        </ol>
        <div className="mt-3 flex flex-wrap gap-2 text-xs">
          <span className="rounded-full bg-[var(--surface-2)] px-3 py-1 text-[var(--teal)]">Silver authoritative from PostgreSQL</span>
          <span className="px-1 py-1 text-[var(--muted)]">Bronze is an independent evidence path</span>
        </div>
      </Panel>
      <div className="mt-4 grid min-w-0 gap-4 xl:grid-cols-[minmax(0,1.4fr)_minmax(240px,0.7fr)]">
        <Panel title="Latest asset runs">
          <div className="grid grid-cols-[1.2fr_0.8fr_1fr] gap-3 pb-2 text-[10px] tracking-[0.14em] text-[var(--muted)] uppercase">
            <span>Asset</span>
            <span>State</span>
            <span>Detail</span>
          </div>
          {[
            { asset: "Analytics publication", state: warehouse?.ready ? "Succeeded" : "Not ready", detail: warehouse?.ready ? "Ready" : warehouse?.reason ?? "No status", ok: Boolean(warehouse?.ready) },
            { asset: "Quality gate", state: gateView?.label ?? "No evidence", detail: gateView?.tone === "ok" ? "Blocked failures" : "See attempt history", ok: gateView?.tone === "ok" },
            { asset: "dbt models", state: dbtView?.label ?? "No evidence", detail: dbt ? formatDuration(dbt.duration_ms) : "No dbt run", ok: dbtView?.tone === "ok" },
            {
              asset: "ClickHouse load",
              state: warehouse?.recent_loads[0]?.status === "published" ? "Succeeded" : loadView?.label ?? "No evidence",
              detail: warehouse?.recent_loads[0]?.status ?? "No load row",
              ok: warehouse?.recent_loads[0]?.status === "published" || loadView?.tone === "ok",
            },
          ].map((row) => (
            <div key={row.asset} className="grid grid-cols-[1.2fr_0.8fr_1fr] gap-3 border-t border-[var(--line)] py-3 text-sm">
              <span>{row.asset}</span>
              <span className={row.ok ? "text-[var(--ok)]" : "text-[var(--warn)]"}>{row.state}</span>
              <span className="text-[var(--muted)]">{row.detail}</span>
            </div>
          ))}
        </Panel>
        <Panel title="Publication lineage">
          <MetaList
            rows={[
              { label: "Warehouse", value: warehouse?.warehouse_publication_id ? <CopyId value={warehouse.warehouse_publication_id} /> : "None" },
              { label: "Silver", value: current ? <CopyId value={current.publication_id} /> : "None" },
              { label: "dbt refs", value: "Audit history preserved" },
              { label: "Recovery", value: warehouse?.ready ? "Verified publication only" : warehouse?.reason ?? "Not verified" },
            ]}
          />
        </Panel>
      </div>
      <ScopeBar>Earlier failures stay available without replacing the active publication.</ScopeBar>
      <div className="mt-4">
        <TechnicalDetails summary="Full attempt history">
        {runs.length === 0 ? (
          <Empty>No warehouse or Dagster attempt is in the returned page.</Empty>
        ) : (
          <DataTable label="Pipeline attempts">
            <thead>
              <tr>
                <Th>Started UTC</Th>
                <Th>Job</Th>
                <Th>Reading</Th>
                <Th>Duration</Th>
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
                    </Td>
                    <Td>{formatDuration(run.duration_ms)}</Td>
                  </tr>
                );
              })}
            </tbody>
          </DataTable>
        )}
        </TechnicalDetails>
      </div>
    </div>
  );
}

function StageCard({ stage }: { stage: { id: string; name: string; detail: string; tone: "info" | "ok" | "warn" | "neutral" } }) {
  const bar = stage.tone === "ok" ? "bg-[var(--ok)]" : stage.tone === "warn" ? "bg-[var(--warn)]" : "bg-[var(--accent)]";
  return (
    <li className="rounded-xl border border-[var(--line)] bg-[var(--background)] px-3 py-3">
      <p className="text-[11px] text-[var(--muted)]">{stage.id}</p>
      <p className="mt-1 text-sm font-medium">{stage.name}</p>
      <p className="mt-1 text-xs text-[var(--muted)]">{stage.detail}</p>
      <div className={`mt-3 h-0.5 rounded-full ${bar}`} />
    </li>
  );
}
