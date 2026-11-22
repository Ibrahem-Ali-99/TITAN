"use client";

import { useEffect, useState } from "react";

import {
  CopyId,
  DataTable,
  Empty,
  ErrorState,
  Kpi,
  LoadingState,
  PageHeader,
  SectionTitle,
  StatusBadge,
  Td,
  Th,
} from "@/components/ui/ops";
import { ApiError, getJson } from "@/lib/api";
import type { LakeStatus, WarehouseStatus } from "@/lib/types";
import { classifyRun, formatCount, formatDuration, formatMoney, formatUtc, hasLaterSuccess } from "@/lib/utils";

const STAGES = [
  "Ingestion",
  "Bronze evidence",
  "Silver",
  "Warehouse load",
  "dbt",
  "Quality gate",
  "Published analytics",
];

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
  const stageNote: Record<string, string> = {
    Ingestion: "PostgreSQL classifications. Not shown as a percentage.",
    "Bronze evidence": status.spark_jobs.find((job) => job.job_name.toLowerCase().includes("bronze"))?.display_state ?? "No bronze job row",
    Silver: current ? "Published Silver version stored" : "No Silver publication",
    "Warehouse load": warehouse?.recent_loads[0]?.status ?? "No load row",
    dbt: latest("warehouse_models")?.state ?? "No dbt run in the returned page",
    "Quality gate": latest("quality_gate")?.state ?? "No quality run in the returned page",
    "Published analytics": warehouse?.ready ? "Serving" : warehouse?.reason ?? "Not serving",
  };

  return (
    <div>
      <PageHeader
        title="Pipelines"
        description="Silver is curated from PostgreSQL dispositions. Bronze evidence is recorded in parallel and is not a prerequisite shown here as a gate. Status text is the stored job state, not a control."
      />
      {error ? <p className="mb-3 text-sm text-amber-100">The latest refresh failed. {error}</p> : null}
      <ol className="mb-4 grid gap-2 md:grid-cols-2 xl:grid-cols-4">
        {STAGES.map((stage, index) => (
          <li key={stage} className="rounded-lg border border-white/8 bg-[#151922] px-3 py-3">
            <p className="text-xs text-zinc-500">{index + 1}</p>
            <p className="mt-1 text-sm font-medium text-zinc-100">{stage}</p>
            <p className="mt-1 text-xs leading-5 text-zinc-400">{stageNote[stage]}</p>
          </li>
        ))}
      </ol>
      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi label="Silver orders" value={formatCount(current?.silver_order_count)} hint="Orders in the latest published Silver version." />
        <Kpi label="Pending Bronze events" value={formatCount(current?.pending_event_count)} hint={status.metric_definitions.pending_event_count ?? "Events still waiting on the Bronze side of that Silver run."} />
        <Kpi label="Failed publications" value={formatCount(status.failed_publications)} hint="Stored failed publication attempts. A later success does not delete them." />
        <Kpi
          label="Warehouse"
          value={warehouse?.ready ? "Ready" : "Not ready"}
          footnote={warehouse?.ready ? `${formatCount(warehouse.order_count)} published orders` : warehouse?.reason ?? "Status was not returned"}
        />
      </div>
      <section className="mb-4 rounded-lg border border-white/8 bg-[#151922] p-4">
        <SectionTitle title="Accepted revenue on the Silver version" detail="Currencies are not added together. This is the Silver publication revenue, which can differ from the serving warehouse until the next exchange." />
        {current && Object.keys(current.accepted_revenue).length > 0 ? (
          <div className="grid gap-3 sm:grid-cols-3">
            {Object.entries(current.accepted_revenue).map(([currency, amount]) => (
              <Kpi key={currency} label={currency} value={formatMoney(amount, currency)} />
            ))}
          </div>
        ) : (
          <Empty>No published Silver revenue is stored.</Empty>
        )}
        <p className="mt-3 text-xs text-zinc-500">
          Identifier conflicts on that Silver run: {formatCount(current?.identifier_conflict_count)}. Bronze rows {formatCount(current?.bronze_record_count)}. Distinct offsets {formatCount(current?.bronze_distinct_offsets)}.
        </p>
      </section>
      <section className="mb-4 rounded-lg border border-white/8 bg-[#151922] p-4">
        <SectionTitle title="Recorded attempts" />
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
      </section>
      <section className="rounded-lg border border-white/8 bg-[#151922] p-4 text-sm text-zinc-300">
        <SectionTitle title="Lineage" />
        <p>Silver <CopyId value={current?.publication_id ?? null} /></p>
        <p className="mt-2">Warehouse <CopyId value={warehouse?.warehouse_publication_id ?? null} /></p>
        <p className="mt-2 text-xs text-zinc-500">
          Silver finished {current ? formatUtc(current.finished_at) : "—"} UTC. Spark jobs reporting:{" "}
          {status.spark_jobs.length === 0
            ? "none"
            : status.spark_jobs.map((job) => `${job.job_name} ${job.display_state}`).join(", ")}
          .
        </p>
      </section>
    </div>
  );
}
