"use client";

import { useEffect, useState } from "react";

import { ProductChart, RevenueChart } from "@/components/ui/charts";
import {
  CopyId,
  Empty,
  ErrorState,
  Kpi,
  LoadingState,
  Notice,
  PageHeader,
  SectionTitle,
  Segmented,
  StatusBadge,
  TechnicalDetails,
} from "@/components/ui/ops";
import { ApiError, getJson } from "@/lib/api";
import type { ProductsResponse, RevenueResponse, WarehouseStatus } from "@/lib/types";
import { formatAge, formatCount, formatMoney, formatUtc } from "@/lib/utils";

const CURRENCIES = ["EUR", "GBP", "USD"] as const;

export function WarehouseDashboard() {
  const [status, setStatus] = useState<WarehouseStatus | null>(null);
  const [revenue, setRevenue] = useState<RevenueResponse | null>(null);
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
          setRevenue(null);
          setProducts(null);
          return;
        }
        const [nextRevenue, nextProducts] = await Promise.all([
          getJson<RevenueResponse>(
            `/api/v1/analytics/revenue?grain=${grain}&currency=${currency}&limit=${grain === "day" ? 60 : 168}`,
          ),
          getJson<ProductsResponse>(`/api/v1/analytics/products?currency=${currency}&limit=12`),
        ]);
        if (!cancelled) {
          setRevenue(nextRevenue);
          setProducts(nextProducts);
        }
      } catch (caught) {
        if (!cancelled) {
          setError(caught instanceof ApiError ? caught.message : "The API request failed.");
          setRevenue(null);
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

  const points =
    revenue?.points.map((point) => ({
      label: grain === "day" ? formatUtc(point.window_start).slice(0, 10) : formatUtc(point.window_start).slice(5, 16),
      orders: point.order_count,
      revenue: formatMoney(point.accepted_revenue, point.currency),
    })) ?? [];
  const productPoints =
    products?.products.map((product) => ({
      product: product.product_id.replace("prd_", ""),
      quantity: product.quantity,
      revenue: formatMoney(product.revenue, product.currency),
    })) ?? [];

  return (
    <div>
      <PageHeader
        title="Warehouse"
        description="Figures are the active ClickHouse publication. Currencies stay separate. A failed load does not replace this snapshot. Commerce rows are synthetic."
        aside={status.ready ? `Published ${formatUtc(status.published_at)} UTC` : undefined}
      />
      {error ? <Notice>The latest refresh failed. {error}</Notice> : null}
      <div className="mb-4 flex flex-wrap items-center gap-2">
        <StatusBadge tone={status.ready ? "ok" : "bad"}>{status.ready ? "Ready" : "Not ready"}</StatusBadge>
        <StatusBadge tone="neutral">Freshness {formatAge(status.freshness_seconds)}</StatusBadge>
      </div>
      {!status.ready ? (
        <section className="rounded-lg border border-white/8 bg-[#151922] p-4 text-sm text-zinc-300">
          <SectionTitle title="No serving snapshot" />
          <p>{status.reason ?? "The warehouse is not ready."}</p>
          <p className="mt-2 text-xs text-zinc-500">Staging rows are not shown as current.</p>
        </section>
      ) : (
        <>
          <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Kpi label="Accepted orders" value={formatCount(status.order_count)} hint="Accepted orders in this publication." />
            <Kpi label="Line items" value={formatCount(status.item_count)} hint="Order line items in this publication." />
            {CURRENCIES.map((code) => (
              <Kpi
                key={code}
                label={`${code} revenue`}
                value={formatMoney(status.accepted_revenue[code], code)}
                hint="Accepted order total for this currency only."
              />
            ))}
          </div>
          <div className="mb-3 flex flex-wrap gap-2">
            <Segmented
              label="Currency"
              value={currency}
              onChange={setCurrency}
              options={CURRENCIES.map((code) => ({ id: code, label: code }))}
            />
            <Segmented
              label="Time grain"
              value={grain}
              onChange={setGrain}
              options={[
                { id: "day", label: "Day" },
                { id: "hour", label: "Hour" },
              ]}
            />
          </div>
          <div className="grid gap-4 xl:grid-cols-2">
            <section className="rounded-lg border border-white/8 bg-[#151922] p-4">
              <SectionTitle
                title={`${currency} order activity`}
                detail={`Event time, ${grain} grain, from the published snapshot.`}
              />
              {points.length === 0 ? <Empty>This currency has no rows in the selected grain.</Empty> : <RevenueChart points={points} />}
            </section>
            <section className="rounded-lg border border-white/8 bg-[#151922] p-4">
              <SectionTitle title={`${currency} product quantity`} detail="Returned product rows for the selected currency." />
              {productPoints.length === 0 ? <Empty>No product lines for this currency.</Empty> : <ProductChart points={productPoints} />}
            </section>
          </div>
        </>
      )}
      <div className="mt-4">
        <TechnicalDetails summary="Publication lineage">
          <p>Warehouse <CopyId value={status.warehouse_publication_id} /></p>
          <p>Silver source <CopyId value={status.silver_publication_id} /></p>
          <p>ClickHouse pointer <CopyId value={status.clickhouse_publication_id} /></p>
          <p className="text-xs text-zinc-500">
            Payments {formatCount(status.payment_count)} · inventory events {formatCount(status.inventory_count)}
          </p>
        </TechnicalDetails>
      </div>
    </div>
  );
}
