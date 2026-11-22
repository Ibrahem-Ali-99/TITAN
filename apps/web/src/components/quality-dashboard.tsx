"use client";

import { useEffect, useState } from "react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError, getJson } from "@/lib/api";

type Summary = {
  available: boolean;
  reason: string | null;
  latest_run_id: string | null;
  latest_status: string | null;
  warehouse_publication_id: string | null;
  failure_count: number | null;
  injected: string | null;
  finished_at: string | null;
};

type Finding = {
  id: number;
  check_id: string;
  severity: string;
  passed: boolean;
  observed: string;
  expected: string;
  business_id: string | null;
  detail: string;
  dataset: string;
};

export function QualityDashboard() {
  const [summary, setSummary] = useState<Summary | null>(null);
  const [findings, setFindings] = useState<Finding[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const next = await getJson<Summary>("/api/v1/quality/summary");
        const failed = await getJson<{ findings: Finding[] }>(
          "/api/v1/quality/findings?failed_only=true&limit=20",
        );
        if (!cancelled) {
          setSummary(next);
          setFindings(failed.findings);
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

  if (loading) {
    return <p className="text-sm text-zinc-400">Loading quality executions…</p>;
  }
  if (!summary) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Quality unavailable</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-red-300">{error}</CardContent>
      </Card>
    );
  }
  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Data Quality</h1>
        <p className="mt-1 max-w-3xl text-sm text-zinc-400">
          Checks run against a warehouse candidate before it is published. Commerce rows are
          synthetic. An injected fault is labeled and does not edit accepted orders.
        </p>
      </div>
      {!summary.available ? (
        <Card>
          <CardHeader>
            <CardTitle>No quality run</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-zinc-300">{summary.reason}</CardContent>
        </Card>
      ) : (
        <Card>
          <CardHeader>
            <CardTitle>Latest execution</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-zinc-300">
            <p>
              {summary.latest_status} · failures {summary.failure_count ?? "—"}
            </p>
            <p className="mt-2 font-mono text-xs">{summary.warehouse_publication_id}</p>
            <p className="mt-2 text-xs text-zinc-500">
              {summary.injected ? `Injected test fault: ${summary.injected}` : "Not an injected run."}
            </p>
          </CardContent>
        </Card>
      )}
      <Card>
        <CardHeader>
          <CardTitle>Failed findings</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-zinc-300">
          {findings.length === 0 ? (
            <p>No failed findings are stored.</p>
          ) : (
            <ul className="space-y-3">
              {findings.map((finding) => (
                <li key={finding.id}>
                  <p>
                    {finding.severity} · {finding.check_id} · {finding.dataset}
                  </p>
                  <p className="text-xs text-zinc-500">
                    observed {finding.observed} · expected {finding.expected}
                    {finding.business_id ? ` · ${finding.business_id}` : ""}
                  </p>
                  <p className="text-xs text-zinc-400">{finding.detail}</p>
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
