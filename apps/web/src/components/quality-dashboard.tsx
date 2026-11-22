"use client";

import { useEffect, useMemo, useState } from "react";

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
  const historicalFailures = runs.filter((run) => run.status !== "passed").length;
  return (
    <div>
      <PageHeader
        title="Data quality"
        description="Current validation separated from audit history"
        aside={currentPassed ? <StatusPill tone="ok">Current passed</StatusPill> : current ? <StatusPill tone="bad">Current failed</StatusPill> : <StatusPill tone="warn">No evidence</StatusPill>}
      />
      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi label="Current checks" value={currentPassed ? "Passed" : current ? "Failed" : "No evidence"} footnote="Serving publication" />
        <Kpi label="Active failures" value={formatCount(current?.failure_count ?? null)} footnote="Blocking incidents" />
        <Kpi label="History" value={historicalFailures > 0 ? "Retained" : "None"} footnote="Test injections" />
        <Kpi label="Reconciliation" value={currentPassed ? "Passed" : current ? "Failed" : "No evidence"} footnote="Currency totals" />
      </div>
      <div className="grid min-w-0 gap-4 xl:grid-cols-[minmax(0,1.4fr)_minmax(240px,0.7fr)]">
        <Panel
          title="Historical incidents"
          detail={`${groups.length} ${groups.length === 1 ? "record" : "records"} shown · previous candidate runs`}
          action={currentPassed ? <StatusPill tone="warn">Not active</StatusPill> : undefined}
        >
          {groups.length === 0 ? (
            <Empty>No failed findings are stored in the returned page.</Empty>
          ) : (
            <ul>
              {groups.map((group) => (
                <li key={`${group.check_id}-${group.dataset}`} className="border-t border-[var(--line)] py-3 first:border-t-0">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <p className="text-sm font-medium">{group.check_id}</p>
                    <StatusBadge tone={group.severity === "critical" ? "bad" : group.severity === "major" ? "warn" : "neutral"}>{group.severity}</StatusBadge>
                  </div>
                  <p className="mt-1 text-xs text-[var(--muted)]">{group.sample}</p>
                  <p className="mt-2 text-xs text-[var(--muted)]">
                    {group.count} occurrences · {group.injected} injected · {group.publications.size} publications
                    {currentPassed ? " · not active" : ""}
                  </p>
                </li>
              ))}
            </ul>
          )}
        </Panel>
        <Panel title="Publication integrity">
          <MetaList
            rows={[
              { label: "Current candidate", value: currentPassed ? "Validated" : current ? "Failed" : "No evidence" },
              { label: "Source reconciliation", value: currentPassed ? "Matched" : current ? "Failed" : "No evidence" },
              { label: "Failed candidates", value: historicalFailures > 0 && currentPassed ? "Blocked before serving" : `${historicalFailures} retained` },
              { label: "Incident visibility", value: currentPassed ? "Historical only" : "See current run" },
            ]}
          />
          {currentPassed ? <StatusPill tone="ok">Active publication passed</StatusPill> : null}
        </Panel>
      </div>
      <ScopeBar>Failed historical candidates are not active incidents. Individual findings stay in the execution record.</ScopeBar>
      <div className="mt-4">
        <TechnicalDetails summary="Execution history and individual findings">
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
              <p className="text-xs text-zinc-400">
                Silver {runs.find((run) => run.run_id === openRun)?.silver_publication_id ?? "—"}
              </p>
            </TechnicalDetails>
          </div>
        ) : null}
        </TechnicalDetails>
      </div>
    </div>
  );
}
