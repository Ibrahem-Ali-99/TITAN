"use client";

import { useEffect, useMemo, useState } from "react";

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
  TechnicalDetails,
  Th,
} from "@/components/ui/ops";
import { ApiError, getJson } from "@/lib/api";
import type { WarehouseStatus } from "@/lib/types";
import { formatCount, formatUtc } from "@/lib/utils";

type QualityRun = {
  run_id: string;
  status: string;
  warehouse_publication_id: string;
  silver_publication_id: string | null;
  failure_count: number;
  injected: string | null;
  finished_at: string | null;
};

type Finding = {
  id: number;
  run_id: string;
  check_id: string;
  severity: string;
  dataset: string;
  observed: string;
  expected: string;
  detail: string;
  passed: boolean;
};

export function QualityDashboard() {
  const [runs, setRuns] = useState<QualityRun[] | null>(null);
  const [findings, setFindings] = useState<Finding[]>([]);
  const [warehouse, setWarehouse] = useState<WarehouseStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [openRun, setOpenRun] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [history, failed, status] = await Promise.all([
          getJson<{ runs: QualityRun[] }>("/api/v1/quality/runs?limit=40"),
          getJson<{ findings: Finding[] }>("/api/v1/quality/findings?failed_only=true&limit=200"),
          getJson<WarehouseStatus>("/api/v1/warehouse/status").catch(() => null),
        ]);
        if (!cancelled) {
          setRuns(history.runs);
          setFindings(failed.findings);
          setWarehouse(status);
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

  const current = useMemo(() => {
    const publication = warehouse?.ready ? warehouse.warehouse_publication_id : null;
    if (!publication || !runs) {
      return null;
    }
    return runs.find((run) => run.warehouse_publication_id === publication) ?? null;
  }, [runs, warehouse]);

  const groups = useMemo(() => {
    const runById = new Map((runs ?? []).map((run) => [run.run_id, run]));
    const grouped = new Map<
      string,
      { check_id: string; severity: string; dataset: string; count: number; injected: number; publications: Set<string>; sample: string }
    >();
    for (const finding of findings) {
      const run = runById.get(finding.run_id);
      const key = `${finding.check_id}|${finding.severity}|${finding.dataset}`;
      const group = grouped.get(key) ?? {
        check_id: finding.check_id,
        severity: finding.severity,
        dataset: finding.dataset,
        count: 0,
        injected: 0,
        publications: new Set<string>(),
        sample: finding.detail || `${finding.observed} vs ${finding.expected}`,
      };
      group.count += 1;
      if (run?.injected) {
        group.injected += 1;
      }
      if (run?.warehouse_publication_id) {
        group.publications.add(run.warehouse_publication_id);
      }
      grouped.set(key, group);
    }
    return [...grouped.values()].sort((left, right) => right.count - left.count);
  }, [findings, runs]);

  if (loading) {
    return <LoadingState label="Loading quality executions…" />;
  }
  if (!runs) {
    return <ErrorState title="Quality unavailable" detail={error} />;
  }

  const currentPassed = current?.status === "passed";
  return (
    <div>
      <PageHeader
        title="Data quality"
        description="The top status is the check recorded for the warehouse that is serving now. Older failures, including intentional injections, stay in the history and are not the live publication."
      />
      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi
          label="Serving publication"
          value={warehouse?.ready ? (currentPassed ? "Validated" : current ? "Failed" : "No evidence") : "Not serving"}
          hint="Looks up a quality run whose publication id equals the ready warehouse. The newest run overall can belong to a candidate that was not exchanged."
        />
        <Kpi
          label="Current result"
          value={current?.status ?? "—"}
          footnote={current?.injected ? `Injected: ${current.injected}` : "Not an injected run"}
        />
        <Kpi label="Current failures" value={formatCount(current?.failure_count ?? null)} />
        <Kpi label="Evaluated" value={current?.finished_at ? formatUtc(current.finished_at) : "—"} footnote="UTC" />
      </div>
      <div className="mb-4">
        <StatusBadge tone={!warehouse?.ready ? "warn" : currentPassed ? "ok" : current ? "bad" : "neutral"}>
          {!warehouse?.ready
            ? "No ready warehouse"
            : currentPassed
              ? "Current snapshot passed blocking checks"
              : current
                ? "Current snapshot has a failed check"
                : "Serving publication has no stored quality run"}
        </StatusBadge>
      </div>
      <section className="mb-4 rounded-lg border border-white/8 bg-[#151922] p-4">
        <SectionTitle title="Execution history" detail="Newest first. Injected rows are test faults, not the serving defect list." />
        {runs.length === 0 ? (
          <Empty>No quality run has been recorded.</Empty>
        ) : (
          <DataTable label="Quality executions">
            <thead>
              <tr>
                <Th>Finished UTC</Th>
                <Th>Status</Th>
                <Th>Failures</Th>
                <Th>Injection</Th>
                <Th>Publication</Th>
                <Th>Open</Th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => {
                const serving = warehouse?.warehouse_publication_id === run.warehouse_publication_id;
                return (
                  <tr key={run.run_id}>
                    <Td className="font-mono text-xs">{formatUtc(run.finished_at)}</Td>
                    <Td>
                      <StatusBadge tone={run.status === "passed" ? "ok" : "bad"}>{run.status}</StatusBadge>
                      {serving ? <span className="ml-2 text-xs text-sky-200">serving</span> : null}
                    </Td>
                    <Td>{run.failure_count}</Td>
                    <Td>{run.injected ?? "—"}</Td>
                    <Td className="font-mono text-xs">{run.warehouse_publication_id.slice(0, 8)}</Td>
                    <Td>
                      <button type="button" className="text-xs text-sky-300" onClick={() => setOpenRun(openRun === run.run_id ? null : run.run_id)}>
                        {openRun === run.run_id ? "Hide" : "Details"}
                      </button>
                    </Td>
                  </tr>
                );
              })}
            </tbody>
          </DataTable>
        )}
        {openRun ? (
          <div className="mt-3">
            <TechnicalDetails summary="Selected execution">
              <p>
                Run <CopyId value={openRun} />
              </p>
              <p className="text-xs text-zinc-500">
                Silver {runs.find((run) => run.run_id === openRun)?.silver_publication_id ?? "—"}
              </p>
            </TechnicalDetails>
          </div>
        ) : null}
      </section>
      <section className="rounded-lg border border-white/8 bg-[#151922] p-4">
        <SectionTitle
          title="Failed checks, grouped"
          detail="Each row is one rule across the returned failed findings. Occurrence count includes historical and injected runs."
        />
        {groups.length === 0 ? (
          <Empty>No failed findings are stored in the returned page.</Empty>
        ) : (
          <DataTable label="Grouped quality findings">
            <thead>
              <tr>
                <Th>Rule</Th>
                <Th>Severity</Th>
                <Th>Dataset</Th>
                <Th>Occurrences</Th>
                <Th>Injected</Th>
                <Th>Publications</Th>
              </tr>
            </thead>
            <tbody>
              {groups.map((group) => (
                <tr key={`${group.check_id}-${group.dataset}`}>
                  <Td>
                    <div>{group.check_id}</div>
                    <div className="mt-1 max-w-md text-xs text-zinc-500">{group.sample}</div>
                  </Td>
                  <Td>
                    <StatusBadge tone={group.severity === "critical" ? "bad" : group.severity === "major" ? "warn" : "neutral"}>
                      {group.severity}
                    </StatusBadge>
                  </Td>
                  <Td>{group.dataset}</Td>
                  <Td>{group.count}</Td>
                  <Td>{group.injected}</Td>
                  <Td>{group.publications.size}</Td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
      </section>
    </div>
  );
}
