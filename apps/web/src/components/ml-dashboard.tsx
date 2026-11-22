"use client";

import { useEffect, useState } from "react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError, getJson } from "@/lib/api";

type Model = {
  run_id: string;
  status: string;
  algorithm: string;
  feature_version: string;
  test_start: string | null;
  test_end: string | null;
  metrics: Record<string, string | number | boolean | null> | null;
  warehouse_publication_id: string;
  detail: string | null;
};

type Forecast = {
  product_id: string;
  target_date: string;
  predicted_quantity: string;
  actual_quantity: string | null;
  kind: string;
};

type Anomaly = {
  product_id: string;
  activity_date: string;
  observed: string;
  baseline: string;
  score: string | null;
  flagged: boolean;
  detail: string;
};

export function MlDashboard() {
  const [models, setModels] = useState<Model[] | null>(null);
  const [reason, setReason] = useState<string | null>(null);
  const [forecasts, setForecasts] = useState<Forecast[]>([]);
  const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const modelBody = await getJson<{ available: boolean; reason: string | null; models: Model[] }>(
          "/api/v1/ml/models?limit=5",
        );
        const forecastBody = await getJson<{ forecasts: Forecast[] }>(
          "/api/v1/ml/forecasts?kind=one_step_evaluation&limit=12",
        );
        const anomalyBody = await getJson<{ anomalies: Anomaly[] }>(
          "/api/v1/ml/anomalies?flagged_only=true&limit=10",
        );
        if (!cancelled) {
          setModels(modelBody.models);
          setReason(modelBody.available ? null : modelBody.reason);
          setForecasts(forecastBody.forecasts);
          setAnomalies(anomalyBody.anomalies);
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
    return <p className="text-sm text-zinc-400">Loading stored model runs…</p>;
  }
  if (!models) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Models unavailable</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-red-300">{error}</CardContent>
      </Card>
    );
  }
  const promoted = models.find((model) => model.status === "promoted") ?? null;
  const maxActual = Math.max(
    1,
    ...forecasts.map((row) => Number(row.actual_quantity ?? row.predicted_quantity)),
  );
  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">ML & Predictions</h1>
        <p className="mt-1 max-w-3xl text-sm text-zinc-400">
          Forecasts are daily product quantities from the published synthetic warehouse. They are
          not a claim about real demand. The chart uses stored one-step evaluation rows.
        </p>
      </div>
      {promoted === null ? (
        <Card>
          <CardHeader>
            <CardTitle>No promoted model</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-zinc-300">
            {reason ?? "Training has not produced a promoted run."}
          </CardContent>
        </Card>
      ) : (
        <Card>
          <CardHeader>
            <CardTitle>{promoted.algorithm}</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-zinc-300">
            <p>
              Test {promoted.test_start} to {promoted.test_end} · {promoted.feature_version}
            </p>
            <p className="mt-2 font-mono text-xs">{promoted.warehouse_publication_id}</p>
            {promoted.metrics ? (
              <ul className="mt-3 space-y-1">
                {Object.entries(promoted.metrics).map(([key, value]) => (
                  <li key={key}>
                    {key}: {String(value)}
                  </li>
                ))}
              </ul>
            ) : null}
            <p className="mt-2 text-xs text-zinc-500">
              A lower model MAE than the seasonal naive is an in-sample synthetic comparison only.
            </p>
          </CardContent>
        </Card>
      )}
      <Card>
        <CardHeader>
          <CardTitle>Actual versus one-step prediction</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm text-zinc-300">
          {forecasts.length === 0 ? (
            <p>No evaluation rows are stored.</p>
          ) : (
            forecasts.map((row) => (
              <div
                key={`${row.product_id}-${row.target_date}`}
                className="grid grid-cols-[92px_1fr_auto] items-center gap-2"
              >
                <span className="font-mono text-[11px] text-zinc-500">{row.target_date}</span>
                <span className="h-2 rounded-full bg-zinc-800">
                  <span
                    className="block h-2 rounded-full bg-sky-700"
                    style={{
                      width: `${(Number(row.actual_quantity ?? 0) / maxActual) * 100}%`,
                    }}
                  />
                </span>
                <span className="font-mono text-xs">
                  actual {row.actual_quantity ?? "—"} · predicted {row.predicted_quantity}
                </span>
              </div>
            ))
          )}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Flagged demand anomalies</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-zinc-300">
          {anomalies.length === 0 ? (
            <p>No flagged statistical anomalies are stored for the promoted run.</p>
          ) : (
            <ul className="space-y-2">
              {anomalies.map((row) => (
                <li key={`${row.product_id}-${row.activity_date}`}>
                  {row.activity_date} · {row.product_id} · observed {row.observed} · baseline{" "}
                  {row.baseline} · score {row.score ?? "—"}
                  <span className="mt-1 block text-xs text-zinc-500">{row.detail}</span>
                </li>
              ))}
            </ul>
          )}
          <p className="mt-3 text-xs text-zinc-500">
            These scores are not data-quality contract failures.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
