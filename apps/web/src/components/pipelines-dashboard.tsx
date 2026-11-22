"use client";

import { useEffect, useState } from "react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError, getJson } from "@/lib/api";
import type { LakeStatus, WarehouseStatus } from "@/lib/types";
import { formatUtc } from "@/lib/utils";

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
    const timer = window.setInterval(() => void load(), 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  if (loading) {
    return <p className="text-sm text-zinc-400">Loading recorded lake publications…</p>;
  }
  if (!status) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Lake status unavailable</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-zinc-300">
          <p>Spark is not reported as healthy when this request fails.</p>
          <pre className="mt-3 overflow-x-auto text-xs text-red-300">{error}</pre>
        </CardContent>
      </Card>
    );
  }

  const current = status.current_publication;
  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Pipelines</h1>
        <p className="mt-1 max-w-3xl text-sm text-zinc-400">
          Spark figures are the last reports stored in PostgreSQL. Warehouse runs are recorded
          attempts. A later success does not erase an earlier failure.
        </p>
      </div>
      {error ? (
        <Card className="border-red-900/80">
          <CardContent className="py-3 text-sm text-red-200">
            The latest refresh failed. The figures below are the previous successful response.
            <pre className="mt-3 overflow-x-auto text-xs">{error}</pre>
          </CardContent>
        </Card>
      ) : null}
      <div className="grid gap-3 md:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle>Current Silver version</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-zinc-300">
            {current ? (
              <>
                <p className="font-mono text-xs">{current.publication_id}</p>
                <p className="mt-2">Published {formatUtc(current.finished_at)}</p>
                <p>Orders {current.silver_order_count ?? "—"}</p>
                <p>Payments {current.silver_payment_count ?? "—"}</p>
                <p>Inventory {current.silver_inventory_count ?? "—"}</p>
                <p>Metric rows {current.order_metric_rows ?? "—"}</p>
              </>
            ) : (
              <p>No Silver publication has been recorded.</p>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Accepted revenue</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-zinc-300">
            {current && Object.keys(current.accepted_revenue).length > 0 ? (
              Object.entries(current.accepted_revenue).map(([currency, amount]) => (
                <p key={currency}>
                  {currency} {amount}
                </p>
              ))
            ) : (
              <p>No published revenue.</p>
            )}
            <p className="mt-2 text-xs text-zinc-500">Currencies are not added together.</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Publication health</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-zinc-300">
            <p>Failed publications {status.failed_publications}</p>
            <p>Pending Bronze events {current?.pending_event_count ?? "—"}</p>
            <p>Bronze rows in that run {current?.bronze_record_count ?? "—"}</p>
            <p>Distinct Kafka offsets {current?.bronze_distinct_offsets ?? "—"}</p>
          </CardContent>
        </Card>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>Warehouse runs</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-zinc-300">
          {warehouse === null ? (
            <p>Warehouse run history was not returned.</p>
          ) : warehouse.recent_runs.length === 0 ? (
            <p>No warehouse or Dagster attempt has been recorded.</p>
          ) : (
            <ul className="space-y-3">
              {warehouse.recent_runs.map((run) => (
                <li key={run.id}>
                  <p>
                    {run.job_name} · {run.state}
                    {run.duration_ms === null ? "" : ` · ${run.duration_ms} ms`}
                  </p>
                  <p className="text-xs text-zinc-500">
                    {formatUtc(run.started_at)}
                    {run.detail ? ` · ${run.detail}` : ""}
                  </p>
                  {run.error_detail ? <p className="text-red-300">{run.error_detail}</p> : null}
                </li>
              ))}
            </ul>
          )}
          {warehouse?.recent_loads.length ? (
            <p className="mt-3 text-xs text-zinc-500">
              Latest load {warehouse.recent_loads[0]?.status} · Silver{" "}
              {warehouse.recent_loads[0]?.silver_publication_id}
            </p>
          ) : null}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Spark jobs</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-zinc-300">
          {status.spark_jobs.length === 0 ? (
            <p>No Spark job has reported status.</p>
          ) : (
            <ul className="space-y-3">
              {status.spark_jobs.map((job) => (
                <li key={job.job_name}>
                  <p>
                    {job.job_name} · {job.display_state}
                  </p>
                  <p className="text-xs text-zinc-500">
                    Updated {formatUtc(job.updated_at)}
                    {job.last_batch_id === null ? "" : ` · batch ${job.last_batch_id}`}
                  </p>
                  {job.error_detail ? <p className="text-red-300">{job.error_detail}</p> : null}
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
