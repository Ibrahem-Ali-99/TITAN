"use client";

import { useEffect, useState } from "react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError, getJson } from "@/lib/api";
import type { AnalyticsOverview, ProductsResponse, RevenueResponse, WarehouseStatus } from "@/lib/types";
import { formatUtc } from "@/lib/utils";

type ReadyData = {
  overview: AnalyticsOverview;
  revenue: RevenueResponse;
  products: ProductsResponse;
};

export function WarehouseDashboard() {
  const [status, setStatus] = useState<WarehouseStatus | null>(null);
  const [data, setData] = useState<ReadyData | null>(null);
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
          setData(null);
          return;
        }
        const [overview, revenue, products] = await Promise.all([
          getJson<AnalyticsOverview>("/api/v1/analytics/overview"),
          getJson<RevenueResponse>("/api/v1/analytics/revenue?grain=hour&limit=168"),
          getJson<ProductsResponse>("/api/v1/analytics/products?limit=20"),
        ]);
        if (!cancelled) {
          setData({ overview, revenue, products });
        }
      } catch (caught) {
        if (!cancelled) {
          setError(caught instanceof ApiError ? caught.message : "The API request failed.");
          setData(null);
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
  }, []);

  if (loading) {
    return <p className="text-sm text-zinc-400">Loading the published warehouse…</p>;
  }
  if (!status) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Warehouse unavailable</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-zinc-300">
          <p>No analytical figures are shown because the status request failed.</p>
          <pre className="mt-3 overflow-x-auto text-xs text-red-300">{error}</pre>
        </CardContent>
      </Card>
    );
  }

  const maxOrders = Math.max(1, ...(data?.revenue.points.map((point) => point.order_count) ?? [1]));
  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Warehouse</h1>
        <p className="mt-1 max-w-3xl text-sm text-zinc-400">
          Figures are the active ClickHouse publication. Commerce rows are synthetic. Currencies
          are not added together. A failed load does not replace this snapshot.
        </p>
      </div>
      {error ? (
        <Card className="border-red-900/80">
          <CardContent className="py-3 text-sm text-red-200">
            The latest refresh failed.
            <pre className="mt-3 overflow-x-auto text-xs">{error}</pre>
          </CardContent>
        </Card>
      ) : null}
      {!status.ready ? (
        <Card>
          <CardHeader>
            <CardTitle>No published warehouse dataset</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-zinc-300">
            <p>{status.reason ?? "The warehouse is not ready."}</p>
            <p className="mt-2 text-xs text-zinc-500">
              Staging tables are not shown as current. Postgres publication{" "}
              {status.warehouse_publication_id ?? "none"} · ClickHouse pointer{" "}
              {status.clickhouse_publication_id ?? "none"}.
            </p>
          </CardContent>
        </Card>
      ) : (
        <>
          <div className="grid gap-3 md:grid-cols-3">
            <Card>
              <CardHeader>
                <CardTitle>Published dataset</CardTitle>
              </CardHeader>
              <CardContent className="text-sm text-zinc-300">
                <p className="font-mono text-xs">{status.warehouse_publication_id}</p>
                <p className="mt-2">Silver {status.silver_publication_id}</p>
                <p>Published {formatUtc(status.published_at)} UTC</p>
                <p>Freshness {status.freshness_seconds ?? "—"} seconds</p>
                <p>Orders {status.order_count ?? "—"}</p>
                <p>Items {status.item_count ?? "—"}</p>
                <p>Payments {status.payment_count ?? "—"}</p>
                <p>Inventory events {status.inventory_count ?? "—"}</p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Accepted revenue</CardTitle>
              </CardHeader>
              <CardContent className="text-sm text-zinc-300">
                {Object.entries(status.accepted_revenue).map(([currency, amount]) => (
                  <p key={currency}>
                    {currency} {amount}
                  </p>
                ))}
                <p className="mt-2 text-xs text-zinc-500">Synthetic order totals, by currency.</p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Query health</CardTitle>
              </CardHeader>
              <CardContent className="text-sm text-zinc-300">
                <p>{data ? "Serving views answered." : "Serving views were not read."}</p>
                <p className="mt-2 text-xs text-zinc-500">
                  ClickHouse pointer {status.clickhouse_publication_id}
                </p>
              </CardContent>
            </Card>
          </div>
          <Card>
            <CardHeader>
              <CardTitle>Event-time order activity</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-sm text-zinc-300">
              {data && data.revenue.points.length > 0 ? (
                data.revenue.points.map((point) => (
                  <div
                    key={`${point.window_start}-${point.currency}`}
                    className="grid grid-cols-[148px_48px_1fr_auto] items-center gap-2"
                  >
                    <span className="font-mono text-[11px] text-zinc-500">
                      {formatUtc(point.window_start)}
                    </span>
                    <span>{point.currency}</span>
                    <span className="h-2 rounded-full bg-zinc-800">
                      <span
                        className="block h-2 rounded-full bg-sky-700"
                        style={{ width: `${(point.order_count / maxOrders) * 100}%` }}
                      />
                    </span>
                    <span className="font-mono text-xs">
                      {point.order_count} · {point.accepted_revenue}
                    </span>
                  </div>
                ))
              ) : (
                <p>The published snapshot has no hourly order rows.</p>
              )}
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Products in the snapshot</CardTitle>
            </CardHeader>
            <CardContent className="text-sm text-zinc-300">
              {data && data.products.products.length > 0 ? (
                <ul className="space-y-1">
                  {data.products.products.map((product) => (
                    <li key={`${product.product_id}-${product.currency}`}>
                      {product.product_id} · {product.quantity} units · {product.currency}{" "}
                      {product.revenue}
                    </li>
                  ))}
                </ul>
              ) : (
                <p>No product lines are in the published snapshot.</p>
              )}
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
