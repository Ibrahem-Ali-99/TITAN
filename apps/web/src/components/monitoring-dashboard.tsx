"use client";

import { useEffect, useState } from "react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError, getJson } from "@/lib/api";

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
};

type Run = {
  id: number;
  job_name: string;
  state: string;
  started_at: string;
  duration_ms: number | null;
  error_detail: string | null;
};

export function MonitoringDashboard() {
  const [overview, setOverview] = useState<Overview | null>(null);
  const [runs, setRuns] = useState<Run[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const next = await getJson<Overview>("/api/v1/monitoring/overview");
        const history = await getJson<{ runs: Run[] }>("/api/v1/monitoring/runs?limit=15");
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

  if (loading) {
    return <p className="text-sm text-zinc-400">Loading pipeline history…</p>;
  }
  if (!overview) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Monitoring unavailable</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-red-300">{error}</CardContent>
      </Card>
    );
  }
  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Monitoring</h1>
        <p className="mt-1 max-w-3xl text-sm text-zinc-400">
          Freshness uses pipeline timestamps. Ingestion lag is the PostgreSQL consumer group.
          Spark progress is its own checkpoint report, not that lag number.
        </p>
      </div>
      <div className="grid gap-3 md:grid-cols-3">
        {overview.freshness.map((stage) => (
          <Card key={stage.stage}>
            <CardHeader>
              <CardTitle>{stage.stage}</CardTitle>
            </CardHeader>
            <CardContent className="text-sm text-zinc-300">
              <p>{stage.age_seconds === null ? "not recorded" : `${stage.age_seconds} seconds`}</p>
              <p className="mt-2 text-xs text-zinc-500">{stage.detail}</p>
            </CardContent>
          </Card>
        ))}
      </div>
      <Card>
        <CardHeader>
          <CardTitle>Ingestion lag</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-zinc-300">
          <p>
            {overview.ingestion_lag.available
              ? `${overview.ingestion_lag.total_lag} records · ${overview.ingestion_lag.consumer_group}`
              : "Lag is not available."}
          </p>
          <p className="mt-2 text-xs text-zinc-500">{overview.ingestion_lag.detail}</p>
          <p className="mt-2 text-xs text-zinc-500">
            Spark: {overview.spark_progress ?? "no Bronze report is stored"}
          </p>
          {overview.latest_failure ? (
            <p className="mt-2 text-red-300">Latest failure: {overview.latest_failure}</p>
          ) : (
            <p className="mt-2 text-xs text-zinc-500">No failed pipeline run is stored.</p>
          )}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Recent attempts</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-zinc-300">
          {runs.length === 0 ? (
            <p>No pipeline attempts are stored.</p>
          ) : (
            <ul className="space-y-2">
              {runs.map((run) => (
                <li key={run.id}>
                  {run.job_name} · {run.state}
                  {run.duration_ms === null ? "" : ` · ${run.duration_ms} ms`}
                  {run.error_detail ? <span className="block text-red-300">{run.error_detail}</span> : null}
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
