"use client";

import { useEffect, useMemo, useState } from "react";

import { EvaluationChart } from "@/components/ui/charts";
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
  SectionTitle,
  ScopeBar,
  StatusPill,
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
  if (loading) {
    return <LoadingState label="Loading stored model runs…" />;
  }
  if (!models) {
    return <ErrorState title="Models unavailable" detail={error} />;
  }

  const strategy = serving?.serving_strategy ?? null;
  const servingMae = strategy?.includes("naive") ? maeBaseline : strategy ? maeModel : null;
  const servingWape = strategy?.includes("naive") ? wapeBaseline : strategy ? wapeModel : null;
  const baselineWins = maeBaseline !== null && maeModel !== null && maeBaseline < maeModel;
  const maeGap =
    baselineWins && maeModel
      ? `${(((maeModel - (maeBaseline ?? 0)) / maeModel) * 100).toFixed(1)}% lower MAE`
      : null;
  const maeMax = Math.max(maeBaseline ?? 0, maeModel ?? 0, 0.001);

  return (
    <div>
      <PageHeader
        title="Demand intelligence"
        description="Product-level demand forecasts. Held-out one-step rows are an evaluation, not a future forecast."
        aside={strategy ? <StatusPill tone="info">{strategy}</StatusPill> : undefined}
      />
      {serving === null ? (
        <Empty>{reason ?? "Training has not produced a serving run."}</Empty>
      ) : (
        <>
          <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Kpi label="Serving MAE" value={servingMae?.toFixed(3) ?? "—"} footnote={strategy ?? "Strategy not recorded"} />
            <Kpi label="Candidate MAE" value={maeModel?.toFixed(3) ?? "—"} footnote={serving.algorithm} />
            <Kpi label="Serving WAPE" value={servingWape?.toFixed(3) ?? "—"} footnote="Held-out" />
            <Kpi label="Evaluation rows" value={formatCount(metricNumber(serving.metrics, "test_rows"))} footnote={`Train ${formatCount(metricNumber(serving.metrics, "train_rows"))}`} />
          </div>
          <div className="grid min-w-0 gap-4 xl:grid-cols-[minmax(0,1.4fr)_minmax(240px,0.7fr)]">
            <Panel
              title={baselineWins ? "Baseline wins" : "Held-out MAE"}
              detail="Lower is better. Values come from the stored model run."
              action={baselineWins ? <StatusPill tone="ok">Serve baseline</StatusPill> : undefined}
            >
              {maeBaseline === null && maeModel === null ? (
                <Empty>The serving run did not include MAE values.</Empty>
              ) : (
                <div className="space-y-4">
                  {maeBaseline !== null ? <MetricBar label="Seasonal naive" value={maeBaseline} max={maeMax} color="var(--teal)" /> : null}
                  {maeModel !== null ? (
                    <MetricBar
                      label={serving.algorithm === "HistGradientBoostingRegressor" ? "Gradient boosting" : serving.algorithm}
                      value={maeModel}
                      max={maeMax}
                      color="var(--warn)"
                    />
                  ) : null}
                  {maeGap ? <p className="text-xs text-[var(--ok)]">{maeGap}</p> : null}
                  {wapeBaseline !== null && wapeModel !== null ? (
                    <p className="text-xs text-[var(--muted)]">
                      WAPE naive {wapeBaseline.toFixed(3)} · candidate {wapeModel.toFixed(3)}
                    </p>
                  ) : null}
                </div>
              )}
            </Panel>
            <Panel title="Why this model?">
              <MetaList
                rows={[
                  { label: "Winner", value: strategy ?? "Not recorded" },
                  { label: "Candidate", value: baselineWins ? "Did not beat baseline" : maeModel !== null && maeBaseline !== null && maeModel < maeBaseline ? "Beat baseline" : "Comparison incomplete" },
                  { label: "Train rows", value: formatCount(metricNumber(serving.metrics, "train_rows")) },
                  { label: "Target", value: "Daily quantity / product" },
                ]}
              />
            </Panel>
          </div>
          <section className="mt-4 rounded-xl border border-[var(--line)] bg-[var(--surface)] p-4">
            <div className="mb-3 flex flex-wrap items-end justify-between gap-3">
              <div>
                <h2 className="text-sm font-medium">Actual vs predicted demand</h2>
                <p className="mt-1 text-xs text-[var(--muted)]">One-step evaluation · daily product units</p>
              </div>
              <StatusPill tone="info">{products.length} products</StatusPill>
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
          <ScopeBar>Synthetic training history limits how far these scores generalize. {serving.decision_rationale ?? ""}</ScopeBar>
          <div className="mt-4">
          <TechnicalDetails summary="Lineage">
            <p>Run <CopyId value={serving.run_id} /></p>
            <p>Training publication <CopyId value={serving.warehouse_publication_id} /></p>
            <p className="text-xs text-zinc-400">
              Features {serving.feature_version}. Train {serving.train_start} to {serving.train_end}. Trained {serving.trained_at}.
            </p>
            <p className="text-xs text-zinc-400">
              Evaluated candidates remain in the model list. Statuses:{" "}
              {models.map((model) => `${model.status}`).join(", ")}.
            </p>
          </TechnicalDetails>
          </div>
        </>
      )}
      <section className="mt-4 rounded-xl border border-[var(--line)] bg-[var(--surface)] p-4">
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

function MetricBar({ label, value, max, color }: { label: string; value: number; max: number; color: string }) {
  return (
    <div className="grid grid-cols-[minmax(0,140px)_1fr_auto] items-center gap-3">
      <span className="text-xs text-[var(--muted)]">{label}</span>
      <div className="h-2 rounded-full bg-[var(--surface-2)]">
        <div className="h-2 rounded-full" style={{ width: `${Math.max(6, (value / max) * 100)}%`, background: color }} />
      </div>
      <span className="font-mono text-xs">{value.toFixed(3)}</span>
    </div>
  );
}
