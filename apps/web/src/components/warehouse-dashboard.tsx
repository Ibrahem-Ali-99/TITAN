"use client";

import { useEffect, useState } from "react";

import { CurrencyOrdersChart, ProductChart } from "@/components/ui/charts";
import {
  CopyId,
  Empty,
  ErrorState,
  LoadingState,
  MetaList,
  Notice,
  PageHeader,
  Panel,
  ScopeBar,
  SectionTitle,
  Segmented,
  StatusPill,
  TechnicalDetails,
} from "@/components/ui/ops";
import { ApiError, getJson } from "@/lib/api";
import type { ProductsResponse, RevenueResponse, WarehouseStatus } from "@/lib/types";
import { formatAge, formatCount, formatMoney, formatUtc } from "@/lib/utils";

const CURRENCIES = ["EUR", "GBP", "USD"] as const;

export function WarehouseDashboard() {
  const [status, setStatus] = useState<WarehouseStatus | null>(null);
  const [series, setSeries] = useState<RevenueResponse[]>([]);
  const [products, setProducts] = useState<ProductsResponse | null>(null);
  const [currency, setCurrency] = useState<(typeof CURRENCIES)[number]>("EUR");
  const [grain, setGrain] = useState<"hour" | "day">("day");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const next = await getJson<WarehouseStatus>("/api/v1/warehouse/status");
        if (cancelled) {
          return;
        }
        setStatus(next);
        setError(null);
        if (!next.ready) {
          setSeries([]);
          setProducts(null);
          return;
        }
        const limit = grain === "day" ? 60 : 168;
        const [nextSeries, nextProducts] = await Promise.all([
          Promise.all(
            CURRENCIES.map((code) =>
              getJson<RevenueResponse>(`/api/v1/analytics/revenue?grain=${grain}&currency=${code}&limit=${limit}`),
            ),
          ),
          getJson<ProductsResponse>(`/api/v1/analytics/products?currency=${currency}&limit=12`),
        ]);
        if (!cancelled) {
          setSeries(nextSeries);
          setProducts(nextProducts);
        }
      } catch (caught) {
        if (!cancelled) {
          setError(caught instanceof ApiError ? caught.message : "The API request failed.");
          setSeries([]);
          setProducts(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }
    void load();
    const timer = window.setInterval(() => void load(), 15000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [currency, grain]);

  if (loading && !status) {
    return <LoadingState label="Loading the published warehouse…" />;
  }
  if (!status) {
    return <ErrorState title="Warehouse unavailable" detail={error} />;
  }

  const merged = new Map<string, { label: string; EUR: number; GBP: number; USD: number }>();
  for (const response of series) {
    for (const point of response.points) {
      const label = grain === "day" ? formatUtc(point.window_start).slice(0, 10) : formatUtc(point.window_start).slice(5, 16);
      const row = merged.get(label) ?? { label, EUR: 0, GBP: 0, USD: 0 };
      if (point.currency === "EUR" || point.currency === "GBP" || point.currency === "USD") {
        row[point.currency] = point.order_count;
      }
      merged.set(label, row);
    }
  }
  const points = [...merged.values()];
  const productPoints =
    products?.products.map((product) => ({
      product: product.product_id.replace("prd_", ""),
      quantity: product.quantity,
      revenue: formatMoney(product.revenue, product.currency),
    })) ?? [];

  return (
    <div>
      <PageHeader
        title="Warehouse analytics"
        description="Published ClickHouse reporting"
        aside={<StatusPill tone={status.ready ? "ok" : "bad"}>{status.ready ? "Validated" : "Not ready"}</StatusPill>}
      />
      {error ? <Notice>The latest refresh failed. {error}</Notice> : null}
      {!status.ready ? (
        <section className="rounded-xl border border-[var(--line)] bg-[var(--surface)] p-4 text-sm text-zinc-300">
          <SectionTitle title="No serving snapshot" />
          <p>{status.reason ?? "The warehouse is not ready."}</p>
          <p className="mt-2 text-xs text-zinc-400">Staging rows are not shown as current.</p>
        </section>
      ) : (
        <>
          <section className="metric-strip warehouse-strip card" aria-label="Warehouse metrics" tabIndex={0}>
            <div><span>Orders</span><b>{formatCount(status.order_count)}</b><small>accepted</small></div><div><span>Items</span><b>{formatCount(status.item_count)}</b><small>order lines</small></div><div><span>Payments</span><b>{formatCount(status.payment_count)}</b><small>accepted</small></div><div><span>Inventory</span><b>{formatCount(status.inventory_count)}</b><small>events</small></div>{CURRENCIES.map((code) => <div key={code}><span>{code} revenue</span><b>{formatMoney(status.accepted_revenue[code], code)}</b><small>currency separated</small></div>)}
          </section>
          <div className="grid min-w-0 gap-4 xl:grid-cols-[minmax(0,1.4fr)_minmax(240px,0.7fr)]">
            <Panel
              title="Order activity by event time"
              detail="Orders per bucket · UTC · recorded data points. Amounts stay in separate currency cards."
              action={
                <Segmented
                  label="Time grain"
                  value={grain}
                  onChange={setGrain}
                  options={[
                    { id: "day", label: "Day" },
                    { id: "hour", label: "Hour" },
                  ]}
                />
              }
            >
              {points.length === 0 ? <Empty>No order rows for this grain.</Empty> : <CurrencyOrdersChart points={points} />}
            </Panel>
            <Panel title="Active snapshot">
              <MetaList
                rows={[
                  { label: "Publication", value: <CopyId value={status.warehouse_publication_id} /> },
                  { label: "Silver source", value: <CopyId value={status.silver_publication_id} /> },
                  { label: "Status", value: status.ready ? "Ready" : "Not ready" },
                  { label: "Query source", value: "ClickHouse serving views" },
                  { label: "Freshness", value: formatAge(status.freshness_seconds) },
                ]}
              />
            </Panel>
          </div>
          <div className="mt-4">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
              <SectionTitle title={`${currency} product quantity`} detail="Returned product rows for the selected currency." />
              <Segmented
                label="Currency"
                value={currency}
                onChange={setCurrency}
                options={CURRENCIES.map((code) => ({ id: code, label: code }))}
              />
            </div>
            {productPoints.length === 0 ? <Empty>No product lines for this currency.</Empty> : <ProductChart points={productPoints} />}
          </div>
        </>
      )}
      <ScopeBar>ClickHouse publication · EUR, GBP and USD are never combined. Published {formatUtc(status.published_at)} UTC.</ScopeBar>
      <div className="mt-4">
        <TechnicalDetails summary="Publication lineage">
          <p>Warehouse <CopyId value={status.warehouse_publication_id} /></p>
          <p>Silver source <CopyId value={status.silver_publication_id} /></p>
          <p>ClickHouse pointer <CopyId value={status.clickhouse_publication_id} /></p>
          <p className="text-xs text-zinc-400">
            Payments {formatCount(status.payment_count)} · inventory events {formatCount(status.inventory_count)}
          </p>
        </TechnicalDetails>
      </div>
    </div>
  );
}
