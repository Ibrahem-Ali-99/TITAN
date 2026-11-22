"use client";

import { useEffect, useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError, fetchOverview, getJson } from "@/lib/api";
import type { HealthReady, IngestionStats, RecentEventsResponse, WarehouseStatus } from "@/lib/types";
import { formatUtc } from "@/lib/utils";

const REFRESH_MS = 5000;

type Snapshot = {
  ready: HealthReady;
  stats: IngestionStats;
  recent: RecentEventsResponse;
  warehouse: WarehouseStatus | null;
  warehouseError: string | null;
  fetchedAt: string;
};

export function OverviewDashboard() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [initialError, setInitialError] = useState<string | null>(null);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;

    async function load(isInitial: boolean) {
      try {
        const [ready, stats, recent] = await fetchOverview();
        let warehouse: WarehouseStatus | null = null;
        let warehouseError: string | null = null;
        try {
          warehouse = await getJson<WarehouseStatus>("/api/v1/warehouse/status");
        } catch (error) {
          warehouseError = error instanceof ApiError ? error.message : "Warehouse status failed.";
        }
        if (cancelled) {
          return;
        }
        setSnapshot({
          ready,
          stats,
          recent,
          warehouse,
          warehouseError,
          fetchedAt: new Date().toISOString(),
        });
        setInitialError(null);
        setRefreshError(null);
      } catch (error) {
        if (cancelled) {
          return;
        }
        const message = error instanceof ApiError ? error.message : "The API request failed.";
        if (isInitial) {
          setInitialError(message);
        } else {
          setRefreshError(message);
        }
      } finally {
        if (!cancelled && isInitial) {
          setLoading(false);
        }
      }
    }

    void load(true);
    const timer = window.setInterval(() => void load(false), REFRESH_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  if (loading) {
    return <p className="text-sm text-zinc-400">Loading persisted ingestion state…</p>;
  }

  if (!snapshot) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>API unavailable</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm text-zinc-300">
          <p>No counts are shown because the overview could not read the API.</p>
          <pre className="overflow-x-auto rounded-md bg-zinc-950 p-3 text-xs text-red-300">
            {initialError}
          </pre>
        </CardContent>
      </Card>
    );
  }

  const { stats, recent, ready } = snapshot;
  const maxActivity = Math.max(1, ...stats.recent_activity.map((point) => point.persisted_events));

  return (
    <div className={refreshError ? "opacity-80" : undefined}>
      <div className="mb-6 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Overview</h1>
          <p className="mt-1 max-w-2xl text-sm text-zinc-400">
            Ingestion counts are PostgreSQL rows. The analytical card is the last published
            ClickHouse snapshot, and it is not the live ingest stream. Commerce data is synthetic.
          </p>
        </div>
        <p className="text-xs text-zinc-500">
          {refreshError ? "Last successful read" : "Updated"} {formatUtc(snapshot.fetchedAt)} UTC
        </p>
      </div>

      {refreshError ? (
        <Card className="mb-4 border-red-900/80">
          <CardContent className="py-3 text-sm text-red-200">
            The latest refresh failed. The figures below are the previous successful response, not
            a live zero. {refreshError}
          </CardContent>
        </Card>
      ) : null}

      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Stat
          label="Persisted events"
          value={stats.persisted_events}
          detail={stats.metric_definitions.persisted_events}
        />
        <Stat
          label="Order events"
          value={stats.persisted_order_created_events}
          detail={stats.metric_definitions.persisted_order_created_events}
        />
        <Stat
          label="Quarantined records"
          value={stats.quarantined_records}
          detail={stats.metric_definitions.quarantined_records}
        />
        <Card>
          <CardHeader>
            <CardTitle>Latest ingestion</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="font-mono text-sm text-zinc-100">
              {formatUtc(stats.most_recent_ingested_at)}
            </p>
            <p className="mt-2 text-xs leading-5 text-zinc-500">
              {stats.metric_definitions.most_recent_ingested_at}
            </p>
            <p className="mt-3 text-xs text-zinc-400">
              PostgreSQL {ready.checks.postgres === "ok" ? "ready" : "not ready"}
            </p>
          </CardContent>
        </Card>
      </div>

      <Card className="mb-4">
        <CardHeader>
          <CardTitle>Published analytical snapshot</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-zinc-300">
          {snapshot.warehouseError ? (
            <p>The warehouse status request failed. Ingestion figures above are unchanged.</p>
          ) : snapshot.warehouse?.ready ? (
            <>
              <p className="font-mono text-xs">{snapshot.warehouse.warehouse_publication_id}</p>
              <p className="mt-2">
                {snapshot.warehouse.order_count ?? "—"} accepted orders · freshness{" "}
                {snapshot.warehouse.freshness_seconds ?? "—"} seconds
              </p>
              {Object.entries(snapshot.warehouse.accepted_revenue).map(([currency, amount]) => (
                <p key={currency}>
                  {currency} {amount}
                </p>
              ))}
              <p className="mt-2 text-xs text-zinc-500">
                Silver {snapshot.warehouse.silver_publication_id}. This is not the live ingestion
                count.
              </p>
            </>
          ) : (
            <p>
              {snapshot.warehouse?.reason ??
                "No validated warehouse publication is available. Ingestion can still be running."}
            </p>
          )}
        </CardContent>
      </Card>

      <div className="grid gap-4 xl:grid-cols-[320px_1fr]">
        <Card>
          <CardHeader>
            <CardTitle>Ingest activity</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <p className="text-xs leading-5 text-zinc-500">{stats.metric_definitions.recent_activity}</p>
            {stats.recent_activity.length === 0 ? (
              <p className="text-sm text-zinc-400">
                No events were persisted in the last {stats.activity_window_minutes} minutes.
              </p>
            ) : (
              <ul className="space-y-2">
                {stats.recent_activity.map((point) => (
                  <li key={point.minute} className="grid grid-cols-[88px_1fr_auto] items-center gap-2">
                    <span className="font-mono text-[11px] text-zinc-500">
                      {formatUtc(point.minute).slice(11, 16)}
                    </span>
                    <span className="h-2 rounded-full bg-zinc-800">
                      <span
                        className="block h-2 rounded-full bg-sky-700"
                        style={{ width: `${(point.persisted_events / maxActivity) * 100}%` }}
                      />
                    </span>
                    <span className="font-mono text-xs text-zinc-300">{point.persisted_events}</span>
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle>Recent persisted events</CardTitle>
            <Badge>{recent.returned} returned</Badge>
          </CardHeader>
          <CardContent className="overflow-x-auto">
            {recent.items.length === 0 ? (
              <p className="text-sm text-zinc-400">No events have been persisted yet.</p>
            ) : (
              <table className="w-full min-w-[760px] text-left text-sm">
                <thead className="text-xs tracking-wide text-zinc-500 uppercase">
                  <tr>
                    <th className="py-2 pr-3 font-medium">Ingested (UTC)</th>
                    <th className="py-2 pr-3 font-medium">Order</th>
                    <th className="py-2 pr-3 font-medium">Total</th>
                    <th className="py-2 pr-3 font-medium">Source</th>
                    <th className="py-2 font-medium">Offset</th>
                  </tr>
                </thead>
                <tbody>
                  {recent.items.map((event) => (
                    <tr key={event.event_id} className="border-t border-zinc-800">
                      <td className="py-2 pr-3 font-mono text-xs text-zinc-300">
                        {formatUtc(event.ingested_at)}
                      </td>
                      <td className="py-2 pr-3">
                        <div className="font-mono text-xs">{event.order_id ?? "—"}</div>
                        <div className="text-xs text-zinc-500">{event.disposition}</div>
                        <div className="text-xs text-zinc-500">{event.event_type}</div>
                      </td>
                      <td className="py-2 pr-3 font-mono text-xs">
                        {event.total_amount ?? "—"} {event.currency ?? ""}
                      </td>
                      <td className="py-2 pr-3 text-xs text-zinc-400">
                        {event.synthetic ? "synthetic" : event.source}
                      </td>
                      <td className="py-2 font-mono text-xs text-zinc-500">
                        {event.kafka_partition}:{event.kafka_offset}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function Stat({ label, value, detail }: { label: string; value: number; detail: string }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{label}</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-3xl font-semibold tracking-tight text-zinc-50">{value}</p>
        <p className="mt-2 text-xs leading-5 text-zinc-500">{detail}</p>
      </CardContent>
    </Card>
  );
}
