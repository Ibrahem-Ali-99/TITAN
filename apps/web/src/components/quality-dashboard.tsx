"use client";

import { useEffect, useMemo, useState } from "react";

import { CopyId, DataTable, Empty, ErrorState, LoadingState, PageHeader, Panel, ScopeBar, StatusBadge, StatusPill, Td, Th } from "@/components/ui/ops";
import { ApiError, getJson } from "@/lib/api";
import type { WarehouseStatus } from "@/lib/types";
import { formatCount, formatUtc } from "@/lib/utils";

type QualityRun = { run_id: string; status: string; warehouse_publication_id: string; silver_publication_id: string | null; failure_count: number; injected: string | null; finished_at: string | null };
type Finding = { id: number; run_id: string; check_id: string; check_type: string; severity: string; dataset: string; observed: string; expected: string; detail: string; passed: boolean; blocks_publication: boolean; business_id: string | null };

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
        const [history, allFindings, status] = await Promise.all([
          getJson<{ runs: QualityRun[] }>("/api/v1/quality/runs?limit=40"),
          getJson<{ findings: Finding[] }>("/api/v1/quality/findings?failed_only=false&limit=200"),
          getJson<WarehouseStatus>("/api/v1/warehouse/status").catch(() => null),
        ]);
        if (!cancelled) { setRuns(history.runs); setFindings(allFindings.findings); setWarehouse(status); setError(null); }
      } catch (caught) { if (!cancelled) setError(caught instanceof ApiError ? caught.message : "The API request failed."); }
      finally { if (!cancelled) setLoading(false); }
    }
    void load();
    return () => { cancelled = true; };
  }, []);

  const current = useMemo(() => {
    const publication = warehouse?.ready ? warehouse.warehouse_publication_id : null;
    return publication && runs ? runs.find((run) => run.warehouse_publication_id === publication) ?? null : null;
  }, [runs, warehouse]);
  const inspectedRun = current ?? runs?.[0] ?? null;
  const inspectedFindings = useMemo(() => {
    const rows = inspectedRun ? findings.filter((finding) => finding.run_id === inspectedRun.run_id) : [];
    return rows.length ? rows : findings.slice(0, 80);
  }, [findings, inspectedRun]);
  const failedFindings = findings.filter((finding) => !finding.passed);
  const blockingFindings = findings.filter((finding) => !finding.passed && finding.blocks_publication);
  const groups = useMemo(() => {
    const map = new Map<string, { check_id: string; check_type: string; dataset: string; total: number; failed: number; blocking: number; sample: string }>();
    for (const finding of findings) {
      const key = `${finding.check_type}|${finding.check_id}|${finding.dataset}`;
      const group = map.get(key) ?? { check_id: finding.check_id, check_type: finding.check_type, dataset: finding.dataset, total: 0, failed: 0, blocking: 0, sample: finding.detail || `${finding.observed} vs ${finding.expected}` };
      group.total += 1; if (!finding.passed) group.failed += 1; if (!finding.passed && finding.blocks_publication) group.blocking += 1;
      map.set(key, group);
    }
    return [...map.values()].sort((left, right) => right.failed - left.failed || right.total - left.total);
  }, [findings]);
  const historicalFailures = (runs ?? []).filter((run) => run.status !== "passed").length;
  const maxFailures = Math.max(1, ...(runs ?? []).map((run) => run.failure_count));

  if (loading) return <LoadingState label="Loading quality executions…" />;
  if (!runs) return <ErrorState title="Quality unavailable" detail={error} />;

  const currentPassed = current?.status === "passed";
  return (
    <div>
      <PageHeader title="Data quality" description="Current validation, rule evidence, and audit history" aside={currentPassed ? <StatusPill tone="ok">Current passed</StatusPill> : current ? <StatusPill tone="bad">Current failed</StatusPill> : <StatusPill tone="warn">No evidence</StatusPill>} />
      <section className="quality-hero card" aria-label="Current quality summary">
        <div className="quality-score"><span>Current publication</span><b>{currentPassed ? "PASS" : current ? "FAIL" : "—"}</b><small>{current?.finished_at ? formatUtc(current.finished_at) : "No current validation"}</small><em>{current ? "Latest validation" : "Serving publication evidence"}</em></div>
        <div className="quality-breakdown"><div><span>Failed checks</span><b>{formatCount(current?.failure_count ?? null)}</b><small>current run</small></div><div><span>Failed findings</span><b>{formatCount(failedFindings.length)}</b><small>returned evidence</small></div><div><span>Blocking findings</span><b>{formatCount(blockingFindings.length)}</b><small>rule metadata</small></div><div><span>Historical failures</span><b>{formatCount(historicalFailures)}</b><small>run history</small></div></div>
      </section>
      {error ? <ScopeBar>The latest refresh failed. Figures below are the previous response. {error}</ScopeBar> : null}

      <Panel title="Validation checks" detail={`Primary structured evidence for ${inspectedRun ? `run ${inspectedRun.run_id.slice(0, 8)}` : "the latest available run"}`} action={<StatusPill tone="info">{inspectedFindings.length} rows</StatusPill>}>
        {inspectedFindings.length ? <DataTable label="Structured quality checks"><thead><tr><Th>Rule</Th><Th>Type</Th><Th>Dataset</Th><Th>Result</Th><Th>Blocking</Th><Th>Observed / expected</Th></tr></thead><tbody>{inspectedFindings.map((finding) => <tr key={finding.id}><Td><b>{finding.check_id}</b>{finding.detail ? <div className="table-subtext">{finding.detail}</div> : null}</Td><Td>{finding.check_type}</Td><Td>{finding.dataset}</Td><Td><StatusBadge tone={finding.passed ? "ok" : "bad"}>{finding.passed ? "Passed" : "Failed"}</StatusBadge></Td><Td>{finding.blocks_publication ? "Yes" : "No"}</Td><Td className="font-mono text-xs">{finding.observed} / {finding.expected}</Td></tr>)}</tbody></DataTable> : <Empty>No structured quality findings are available for the current run.</Empty>}
      </Panel>

      <div className="two-col mt-4">
        <Panel title="Validation history" detail="Failure counts from recorded quality runs; no interpolation or score is applied">
          <div className="validation-history">{runs.slice(0, 12).map((run) => <div className="validation-history-row" key={run.run_id}><div><span>{run.finished_at ? formatUtc(run.finished_at).slice(0, 16) : "No finish time"}</span><b>{run.failure_count} failures</b><StatusBadge tone={run.status === "passed" ? "ok" : "bad"}>{run.status}</StatusBadge></div><i><b style={{ width: `${run.failure_count ? Math.max(8, (run.failure_count / maxFailures) * 100) : 4}%` }} /></i></div>)}</div>
        </Panel>
        <Panel title="Validation group breakdown" detail="Actual rule categories and datasets returned by the quality API">
          {groups.length ? <div className="validation-groups-list">{groups.map((group) => <div key={`${group.check_type}-${group.check_id}-${group.dataset}`}><div><b>{group.check_id}</b><span>{group.check_type} · {group.dataset}</span></div><strong className={group.failed ? "bad-text" : "ok-text"}>{group.failed}/{group.total} failed</strong><small>{group.blocking} blocking · {group.sample}</small></div>)}</div> : <Empty>No validation groups are available.</Empty>}
        </Panel>
      </div>

      <div className="two-col mt-4">
        <Panel title="Historical incidents" detail="Failed findings from earlier or non-serving candidates">
          {groups.filter((group) => group.failed > 0).length ? <ul>{groups.filter((group) => group.failed > 0).slice(0, 8).map((group) => <li key={`incident-${group.check_id}-${group.dataset}`} className="history-item"><div><b>{group.check_id}</b><StatusBadge tone={group.blocking ? "bad" : "warn"}>{group.blocking ? "Blocking" : "Review"}</StatusBadge></div><p>{group.sample}</p><small>{group.failed} failed finding{group.failed === 1 ? "" : "s"} · {group.dataset}</small></li>)}</ul> : <Empty>No failed findings are stored in the returned page.</Empty>}
        </Panel>
        <Panel title="Publication integrity" detail="Current publication and source identifiers">
          <div className="meta-list"><div><span>Current candidate</span><b>{currentPassed ? "Validated" : current ? "Failed" : "No evidence"}</b></div><div><span>Warehouse publication</span><b><CopyId value={current?.warehouse_publication_id ?? warehouse?.warehouse_publication_id ?? null} /></b></div><div><span>Silver publication</span><b><CopyId value={current?.silver_publication_id ?? warehouse?.silver_publication_id ?? null} /></b></div><div><span>Active blocking state</span><b>{currentPassed ? "No" : blockingFindings.length ? "Review" : "Not measured"}</b></div></div>
        </Panel>
      </div>
      <ScopeBar>Failed historical candidates are not active incidents. Pass/fail and blocking values are shown only when returned by the quality API.</ScopeBar>

      <Panel title="Execution history" detail="Detailed recorded quality runs below the primary validation experience">
        {runs.length ? <DataTable label="Quality execution history"><thead><tr><Th>Finished UTC</Th><Th>Status</Th><Th>Failures</Th><Th>Injection</Th><Th>Publication</Th><Th>Open</Th></tr></thead><tbody>{runs.map((run) => { const serving = warehouse?.warehouse_publication_id === run.warehouse_publication_id; return <tr key={run.run_id}><Td className="font-mono text-xs">{formatUtc(run.finished_at)}</Td><Td><StatusBadge tone={run.status === "passed" ? "ok" : "bad"}>{run.status}</StatusBadge>{serving ? <span className="ml-2 text-xs text-[var(--accent)]">serving</span> : null}</Td><Td>{run.failure_count}</Td><Td>{run.injected ?? "—"}</Td><Td className="font-mono text-xs">{run.warehouse_publication_id.slice(0, 8)}</Td><Td><button type="button" className="text-xs text-[var(--accent)]" onClick={() => setOpenRun(openRun === run.run_id ? null : run.run_id)}>{openRun === run.run_id ? "Hide" : "Details"}</button></Td></tr>; })}</tbody></DataTable> : <Empty>No quality run has been recorded.</Empty>}
        {openRun ? <div className="selected-run"><p>Run <CopyId value={openRun} /></p><p>Silver publication {runs.find((run) => run.run_id === openRun)?.silver_publication_id ?? "—"}</p></div> : null}
      </Panel>
    </div>
  );
}
