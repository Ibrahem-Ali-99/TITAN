"use client";

import { useEffect, useMemo, useState } from "react";

import { EvaluationChart, MetricCompareChart } from "@/components/ui/charts";
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
import { formatCount } from "@/lib/utils";

type Model = {
  run_id: string;
  status: string;
  algorithm: string;
  serving_strategy: string | null;
  decision_rationale: string | null;
  feature_version: string;
  train_start: string | null;
  train_end: string | null;
  test_start: string | null;
  test_end: string | null;
  metrics: Record<string, string | number | boolean | null> | null;
  warehouse_publication_id: string;
  trained_at: string;
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

function metricNumber(metrics: Model["metrics"], key: string): number | null {
  const value = metrics?.[key];
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string" && value !== "" && Number.isFinite(Number(value))) {
    return Number(value);
  }
  return null;
}

export function MlDashboard() {
  const [models, setModels] = useState<Model[] | null>(null);
  const [reason, setReason] = useState<string | null>(null);
  const [forecasts, setForecasts] = useState<Forecast[]>([]);
  const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
  const [product, setProduct] = useState<string>("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [modelBody, forecastBody, anomalyBody] = await Promise.all([
          getJson<{ available: boolean; reason: string | null; models: Model[] }>("/api/v1/ml/models?limit=8"),
          getJson<{ forecasts: Forecast[] }>("/api/v1/ml/forecasts?kind=one_step_evaluation&limit=200"),
          getJson<{ anomalies: Anomaly[] }>("/api/v1/ml/anomalies?flagged_only=true&limit=20"),
        ]);
        if (!cancelled) {
          setModels(modelBody.models);
          setReason(modelBody.available ? null : modelBody.reason);
          setForecasts(forecastBody.forecasts);
          setAnomalies(anomalyBody.anomalies);
          const first = forecastBody.forecasts[0]?.product_id ?? "";
          setProduct((current) => current || first);
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

  const serving =
    models?.find((model) => model.status === "serving") ??
    models?.find((model) => model.status === "promoted") ??
    null;
  const products = useMemo(
    () => [...new Set(forecasts.map((row) => row.product_id))].sort(),
    [forecasts],
  );
  const series = forecasts
    .filter((row) => row.product_id === product)
    .slice()
    .sort((left, right) => left.target_date.localeCompare(right.target_date))
    .map((row) => ({
      date: row.target_date.slice(5),
      actual: row.actual_quantity === null ? null : Number(row.actual_quantity),
      predicted: Number(row.predicted_quantity),
    }));
  const maeBaseline = serving ? metricNumber(serving.metrics, "mae_seasonal_naive") : null;
  const maeModel = serving ? metricNumber(serving.metrics, "mae_model") : null;
  const wapeBaseline = serving ? metricNumber(serving.metrics, "wape_seasonal_naive") : null;
  const wapeModel = serving ? metricNumber(serving.metrics, "wape_model") : null;
  const comparison = [
    maeBaseline !== null && maeModel !== null ? { metric: "MAE", baseline: maeBaseline, candidate: maeModel } : null,
    wapeBaseline !== null && wapeModel !== null
      ? { metric: "WAPE", baseline: wapeBaseline, candidate: wapeModel }
      : null,
  ].filter((row): row is { metric: string; baseline: number; candidate: number } => row !== null);

  if (loading) {
    return <LoadingState label="Loading stored model runs…" />;
  }
  if (!models) {
    return <ErrorState title="Models unavailable" detail={error} />;
  }

  return (
    <div>
      <PageHeader
        title="ML and predictions"
        description="The served method is the stored decision. Held-out one-step rows are an evaluation, not a future forecast. The series is synthetic, so a lower error here is not evidence about a real catalog."
      />
      {serving === null ? (
        <Empty>{reason ?? "Training has not produced a serving run."}</Empty>
      ) : (
        <>
          <div className="mb-3 flex flex-wrap gap-2">
            <StatusBadge tone="info">Serving {serving.serving_strategy ?? "unlabeled"}</StatusBadge>
            <StatusBadge tone="neutral">Candidate {serving.algorithm}</StatusBadge>
          </div>
          <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Kpi label="Serving method" value={serving.serving_strategy ?? "Not recorded"} hint={serving.decision_rationale ?? "No rationale was stored."} />
            <Kpi label="Naive MAE" value={maeBaseline?.toFixed(3) ?? "—"} footnote="Held-out seasonal naive" />
            <Kpi label="Candidate MAE" value={maeModel?.toFixed(3) ?? "—"} footnote={serving.algorithm} />
            <Kpi
              label="Evaluation rows"
              value={formatCount(metricNumber(serving.metrics, "test_rows"))}
              footnote={`Train ${formatCount(metricNumber(serving.metrics, "train_rows"))} · ${serving.test_start ?? "—"} to ${serving.test_end ?? "—"}`}
            />
          </div>
          {serving.decision_rationale ? (
            <p className="mb-4 max-w-3xl text-sm leading-6 text-zinc-300">{serving.decision_rationale}</p>
          ) : null}
          <section className="mb-4 rounded-lg border border-white/8 bg-[#151922] p-4">
            <SectionTitle title="Held-out error" detail="Lower is better. WAPE is omitted by the API when the actual sum is zero." />
            {comparison.length === 0 ? (
              <Empty>The serving run did not include comparable MAE or WAPE values.</Empty>
            ) : (
              <MetricCompareChart rows={comparison} candidateName={serving.algorithm} />
            )}
          </section>
          <section className="mb-4 rounded-lg border border-white/8 bg-[#151922] p-4">
            <div className="mb-3 flex flex-wrap items-end justify-between gap-3">
              <SectionTitle
                title="One-step evaluation"
                detail="Actual quantity uses the observed day. The prediction uses only earlier information. This chart is not the recursive forward week."
              />
              <label className="text-xs text-zinc-400">
                Product
                <select
                  className="ml-2 rounded border border-white/10 bg-[#0e1116] px-2 py-1 font-mono"
                  value={product}
                  onChange={(event) => setProduct(event.target.value)}
                >
                  {products.map((id) => (
                    <option key={id} value={id}>
                      {id}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            {series.length === 0 ? <Empty>No one-step rows are stored for this product.</Empty> : <EvaluationChart points={series} />}
          </section>
          <TechnicalDetails summary="Lineage">
            <p>Run <CopyId value={serving.run_id} /></p>
            <p>Training publication <CopyId value={serving.warehouse_publication_id} /></p>
            <p className="text-xs text-zinc-500">
              Features {serving.feature_version}. Train {serving.train_start} to {serving.train_end}. Trained {serving.trained_at}.
            </p>
            <p className="text-xs text-zinc-500">
              Evaluated candidates remain in the model list. Statuses:{" "}
              {models.map((model) => `${model.status}`).join(", ")}.
            </p>
          </TechnicalDetails>
        </>
      )}
      <section className="mt-4 rounded-lg border border-white/8 bg-[#151922] p-4">
        <SectionTitle
          title="Statistical anomalies"
          detail="Rolling median scores on observed history. These are not quality-contract failures and they are not forecast errors."
        />
        {anomalies.length === 0 ? (
          <Empty>No flagged anomalies are stored for the returned page.</Empty>
        ) : (
          <DataTable label="Flagged anomalies">
            <thead>
              <tr>
                <Th>Date</Th>
                <Th>Product</Th>
                <Th>Observed</Th>
                <Th>Baseline</Th>
                <Th>Score</Th>
              </tr>
            </thead>
            <tbody>
              {anomalies.map((row) => (
                <tr key={`${row.product_id}-${row.activity_date}`}>
                  <Td>{row.activity_date}</Td>
                  <Td className="font-mono text-xs">{row.product_id}</Td>
                  <Td>{row.observed}</Td>
                  <Td>{row.baseline}</Td>
                  <Td>{row.score ?? "—"}</Td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
      </section>
    </div>
  );
}
