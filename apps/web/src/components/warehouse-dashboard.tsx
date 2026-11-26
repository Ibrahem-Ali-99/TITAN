"use client";

import { useEffect, useState } from "react";

import { CurrencyOrdersChart, ProductPerformanceChart } from "@/components/ui/charts";
import {
  CopyId,
  DataTable,
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
  StatusBadge,
  StatusPill,
  Td,
  Th,
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
  const points = [...merged.values()].sort((left, right) => left.label.localeCompare(right.label));
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
          <div className="two-col warehouse-charts">
            <Panel
              title="Order activity by event time"
              detail="Observed event-time buckets · UTC · missing intervals are not interpolated."
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
            <Panel title="Product performance" detail="Horizontal quantity view · selected currency only" action={<Segmented label="Currency" value={currency} onChange={setCurrency} options={CURRENCIES.map((code) => ({ id: code, label: code }))} />}>
              {productPoints.length === 0 ? <Empty>No product lines for this currency.</Empty> : <ProductPerformanceChart points={productPoints} />}
            </Panel>
          </div>
          <div className="two-col mt-4">
            <Panel title="Revenue by currency" detail="Accepted snapshot totals; currencies remain separate">
              <div className="revenue-list">{CURRENCIES.map((code, index) => <div className="revenue-row" key={code}><span className={`currency c${index}`}>{code}</span><div><b>{formatMoney(status.accepted_revenue[code], code)}</b><span>Accepted revenue · no FX conversion</span></div></div>)}</div>
            </Panel>
            <Panel title="Active snapshot" detail="Current ClickHouse serving pointer">
              <MetaList rows={[{ label: "Publication", value: <CopyId value={status.warehouse_publication_id} /> }, { label: "Silver source", value: <CopyId value={status.silver_publication_id} /> }, { label: "Status", value: status.ready ? "Ready" : "Not ready" }, { label: "Query source", value: "ClickHouse serving views" }, { label: "Freshness", value: formatAge(status.freshness_seconds) }]} />
            </Panel>
          </div>
          <div className="two-col mt-4">
            <Panel title="Publication history" detail="Recorded warehouse loads; the active publication remains explicit">
              {status.recent_loads.length ? <DataTable label="Warehouse publication history"><thead><tr><Th>Finished UTC</Th><Th>Status</Th><Th>Orders</Th><Th>Publication</Th></tr></thead><tbody>{status.recent_loads.slice(0, 8).map((load) => <tr key={load.load_id}><Td className="font-mono text-xs">{formatUtc(load.finished_at ?? load.started_at)}</Td><Td><StatusBadge tone={load.status === "succeeded" || load.status === "published" ? "ok" : load.status === "failed" ? "bad" : "neutral"}>{load.status}</StatusBadge></Td><Td>{formatCount(load.order_count)}</Td><Td className="font-mono text-xs">{load.warehouse_publication_id.slice(0, 8)}</Td></tr>)}</tbody></DataTable> : <Empty>No warehouse loads are recorded.</Empty>}
            </Panel>
            <Panel title="Publication lineage" detail="Copyable identifiers from the current response">
              <div className="lineage-flow"><div className="lineage-node"><span>01</span><b>Silver</b><small><CopyId value={status.silver_publication_id} /></small></div><i className="lineage-connector" aria-hidden>→</i><div className="lineage-node"><span>02</span><b>Warehouse</b><small><CopyId value={status.warehouse_publication_id} /></small></div><i className="lineage-connector" aria-hidden>→</i><div className="lineage-node"><span>03</span><b>ClickHouse</b><small><CopyId value={status.clickhouse_publication_id} /></small></div></div>
            </Panel>
          </div>
        </>
      )}
      <ScopeBar>ClickHouse publication · EUR, GBP and USD are never combined. Published {formatUtc(status.published_at)} UTC.</ScopeBar>
    </div>
  );
}
