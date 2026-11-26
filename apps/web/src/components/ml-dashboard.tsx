"use client";

import { useEffect, useMemo, useState } from "react";

import { EvaluationChart } from "@/components/ui/charts";
import { CopyId, DataTable, Empty, ErrorState, Kpi, LoadingState, PageHeader, Panel, ScopeBar, StatusBadge, StatusPill, Td, TechnicalDetails, Th } from "@/components/ui/ops";
import { ApiError, getJson } from "@/lib/api";
import { formatCount, formatUtc } from "@/lib/utils";

type Model = { run_id: string; status: string; algorithm: string; serving_strategy: string | null; decision_rationale: string | null; feature_version: string; train_start: string | null; train_end: string | null; test_start: string | null; test_end: string | null; metrics: Record<string, string | number | boolean | null> | null; warehouse_publication_id: string; trained_at: string };
type Forecast = { product_id: string; target_date: string; predicted_quantity: string; actual_quantity: string | null; kind: string };
type Anomaly = { product_id: string; activity_date: string; observed: string; baseline: string; score: string | null; flagged: boolean; detail: string };

function metricNumber(metrics: Model["metrics"], key: string): number | null {
  const value = metrics?.[key];
  if (typeof value === "number") return value;
  if (typeof value === "string" && value !== "" && Number.isFinite(Number(value))) return Number(value);
  return null;
}

export function MlDashboard() {
  const [models, setModels] = useState<Model[] | null>(null);
  const [reason, setReason] = useState<string | null>(null);
  const [forecasts, setForecasts] = useState<Forecast[]>([]);
  const [futureForecasts, setFutureForecasts] = useState<Forecast[]>([]);
  const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
  const [product, setProduct] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [modelBody, forecastBody, futureBody, anomalyBody] = await Promise.all([
          getJson<{ available: boolean; reason: string | null; models: Model[] }>("/api/v1/ml/models?limit=8"),
          getJson<{ forecasts: Forecast[] }>("/api/v1/ml/forecasts?kind=one_step_evaluation&limit=200"),
          getJson<{ forecasts: Forecast[] }>("/api/v1/ml/forecasts?kind=forecast&limit=200"),
          getJson<{ anomalies: Anomaly[] }>("/api/v1/ml/anomalies?flagged_only=true&limit=20"),
        ]);
        if (!cancelled) { setModels(modelBody.models); setReason(modelBody.available ? null : modelBody.reason); setForecasts(forecastBody.forecasts); setFutureForecasts(futureBody.forecasts); setAnomalies(anomalyBody.anomalies); setProduct((current) => current || preferredProduct(forecastBody.forecasts) || forecastBody.forecasts[0]?.product_id || ""); setError(null); }
      } catch (caught) { if (!cancelled) setError(caught instanceof ApiError ? caught.message : "The API request failed."); }
      finally { if (!cancelled) setLoading(false); }
    }
    void load();
    return () => { cancelled = true; };
  }, []);

  const serving = models?.find((model) => model.status === "serving") ?? models?.find((model) => model.status === "promoted") ?? null;
  const products = useMemo(() => [...new Set(forecasts.map((row) => row.product_id))].sort(), [forecasts]);
  const series = forecasts.filter((row) => row.product_id === product).slice().sort((a, b) => a.target_date.localeCompare(b.target_date)).map((row) => ({ date: row.target_date.slice(5), actual: row.actual_quantity === null ? null : Number(row.actual_quantity), predicted: Number(row.predicted_quantity) }));
  if (loading) return <LoadingState label="Loading stored model runs…" />;
  if (!models) return <ErrorState title="Models unavailable" detail={error} />;

  const maeBaseline = serving ? metricNumber(serving.metrics, "mae_seasonal_naive") : null;
  const maeModel = serving ? metricNumber(serving.metrics, "mae_model") : null;
  const wapeBaseline = serving ? metricNumber(serving.metrics, "wape_seasonal_naive") : null;
  const wapeModel = serving ? metricNumber(serving.metrics, "wape_model") : null;
  const strategy = serving?.serving_strategy ?? null;
  const servingMae = strategy?.includes("naive") ? maeBaseline : strategy ? maeModel : null;
  const servingWape = strategy?.includes("naive") ? wapeBaseline : strategy ? wapeModel : null;
  const baselineWins = maeBaseline !== null && maeModel !== null && maeBaseline < maeModel;
  const maeMax = Math.max(maeBaseline ?? 0, maeModel ?? 0, 0.001);

  return <div>
    <PageHeader title="Machine Learning" description="Demand forecasting, measured model selection, and evaluation intelligence." aside={strategy ? <StatusPill tone="info">{formatStrategy(strategy)}</StatusPill> : undefined} />
    {serving ? <div className="model-banner card"><div className="model-mark">ML</div><div><span>ACTIVE FORECASTING STRATEGY</span><h2>{formatStrategy(strategy ?? serving.algorithm)}</h2><p className="model-rationale">{serving.decision_rationale ?? "Serving strategy is recorded with the model run."}</p></div><div className="model-meta"><span>Latest run <b title={serving.trained_at}>{formatInstant(serving.trained_at)}</b></span><span>Evaluation rows <b>{forecasts.length}</b></span></div><StatusPill tone="ok">Serving</StatusPill></div> : <Empty>{reason ?? "Training has not produced a serving run."}</Empty>}
    {serving ? <>
      <div className="kpi-grid"><Kpi label="Forecast rows" value={formatCount(forecasts.length)} footnote={`${products.length} technical product IDs`} /><Kpi label="Serving model MAE" value={servingMae?.toFixed(3) ?? "—"} footnote={strategy ?? "Not recorded"} tone="green" /><Kpi label="Serving WAPE" value={servingWape?.toFixed(3) ?? "—"} footnote="Held-out evaluation" tone="cyan" /><Kpi label="Evaluation rows" value={formatCount(metricNumber(serving.metrics, "test_rows"))} footnote={`Train ${formatCount(metricNumber(serving.metrics, "train_rows"))}`} tone="amber" /></div>
      <div className="two-col ml-charts"><Panel title="Actual, predicted and evaluated demand" detail="One-step evaluation · daily product units"><div className="mb-3 flex flex-wrap items-center justify-between gap-3"><StatusPill tone="info">{products.length} products</StatusPill><label className="text-xs text-[var(--muted)]">Product <select className="ml-2 rounded border border-[var(--line)] bg-[var(--background)] px-2 py-1" value={product} onChange={(event) => setProduct(event.target.value)}>{products.map((id) => <option key={id} value={id}>{formatProductId(id)}</option>)}</select></label></div>{series.length ? <EvaluationChart points={series} /> : <Empty>No one-step rows are stored for this product.</Empty>}</Panel><Panel title="Model comparison" detail="Measured holdout error; lower is better"><div className="model-compare"><div><span>Seasonal naive <StatusPill tone={baselineWins ? "ok" : "info"}>{baselineWins ? "Serving" : "Baseline"}</StatusPill></span><b>{maeBaseline?.toFixed(3) ?? "—"} MAE</b><div><i style={{ width: `${maeBaseline === null ? 0 : Math.max(8, maeBaseline / maeMax * 100)}%` }} /></div></div><div><span>{formatAlgorithm(serving.algorithm)} <StatusPill tone="info">Candidate</StatusPill></span><b>{maeModel?.toFixed(3) ?? "—"} MAE</b><div><i style={{ width: `${maeModel === null ? 0 : Math.max(8, maeModel / maeMax * 100)}%` }} /></div></div></div><p className="evaluation-note">Selection uses measured holdout MAE. Missing future or evaluation values are not substituted.</p>{wapeBaseline !== null && wapeModel !== null ? <p className="text-xs text-[var(--muted)]">WAPE · baseline {wapeBaseline.toFixed(3)} · candidate {wapeModel.toFixed(3)}</p> : null}</Panel></div>
      <ScopeBar>Synthetic training history limits how far these scores generalize. {serving.decision_rationale ?? ""}</ScopeBar>
      <div className="two-col"><Panel title="Evaluation results" detail="Stored one-step rows · technical IDs remain copyable through the underlying value"><DataTable label="Evaluation results"><thead><tr><Th>Product ID</Th><Th>Date</Th><Th>Actual</Th><Th>Predicted</Th><Th>Kind</Th></tr></thead><tbody>{forecasts.slice(0, 20).map((row) => <tr key={`${row.product_id}-${row.target_date}`}><Td className="font-mono text-xs" title={row.product_id}>{formatProductId(row.product_id)}</Td><Td>{row.target_date}</Td><Td>{row.actual_quantity ?? "—"}</Td><Td>{row.predicted_quantity}</Td><Td><StatusBadge tone="neutral">Evaluation</StatusBadge></Td></tr>)}</tbody></DataTable></Panel><Panel title="Model run history" detail="Training/evaluation records, with serving state explicit"><DataTable label="Model runs"><thead><tr><Th>Run</Th><Th>Algorithm</Th><Th>Status</Th><Th>Serving</Th><Th>Trained</Th></tr></thead><tbody>{models.map((model) => <tr key={model.run_id}><Td><CopyId value={model.run_id} /></Td><Td>{formatAlgorithm(model.algorithm)}</Td><Td><StatusBadge tone={model.status === "serving" || model.status === "promoted" ? "ok" : "neutral"}>{model.status}</StatusBadge></Td><Td>{model.run_id === serving.run_id ? "Current" : "—"}</Td><Td title={model.trained_at}>{formatInstant(model.trained_at)}</Td></tr>)}</tbody></DataTable></Panel></div>
      <Panel title="Future forecasts" detail="Recursive forecast rows returned by the model API; evaluation rows are kept separate"><>{futureForecasts.length ? <DataTable label="Future forecasts"><thead><tr><Th>Product ID</Th><Th>Target date</Th><Th>Predicted quantity</Th><Th>Kind</Th></tr></thead><tbody>{futureForecasts.slice(0, 20).map((row) => <tr key={`${row.product_id}-${row.target_date}`}><Td className="font-mono text-xs" title={row.product_id}>{formatProductId(row.product_id)}</Td><Td>{row.target_date}</Td><Td>{row.predicted_quantity}</Td><Td><StatusBadge tone="info">Forecast</StatusBadge></Td></tr>)}</tbody></DataTable> : <Empty>No future forecast rows are available from the model API.</Empty>}</></Panel>
      <TechnicalDetails summary="Model lineage"><p>Run <CopyId value={serving.run_id} /></p><p>Training publication <CopyId value={serving.warehouse_publication_id} /></p><p className="text-xs text-[var(--muted)]">Features {serving.feature_version}. Train {serving.train_start} to {serving.train_end}.</p></TechnicalDetails>
    </> : null}
    <Panel title="Statistical anomalies" detail="Flagged anomalies from observed history"><>{anomalies.length ? <DataTable label="Flagged anomalies"><thead><tr><Th>Date</Th><Th>Product</Th><Th>Observed</Th><Th>Baseline</Th><Th>Score</Th></tr></thead><tbody>{anomalies.map((row) => <tr key={`${row.product_id}-${row.activity_date}`}><Td>{row.activity_date}</Td><Td className="font-mono text-xs">{row.product_id}</Td><Td>{row.observed}</Td><Td>{row.baseline}</Td><Td>{row.score ?? "—"}</Td></tr>)}</tbody></DataTable> : <Empty>No flagged anomalies are stored for the returned page.</Empty>}</></Panel>
  </div>;
}

function formatAlgorithm(value: string) { return value.replaceAll("HistGradientBoostingRegressor", "HistGradientBoosting Regressor").replaceAll("_", " "); }
function formatStrategy(value: string) { return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase()); }
function formatProductId(value: string) { return value.replace(/^prd_/, ""); }
function formatInstant(value: string) { const date = new Date(value); return Number.isNaN(date.getTime()) ? value : formatUtc(value).replace(" UTC", ""); }
function preferredProduct(rows: Forecast[]) { return rows.find((row) => row.actual_quantity !== null && Number(row.actual_quantity) > 0)?.product_id ?? rows.find((row) => Number(row.predicted_quantity) > 0)?.product_id ?? null; }
