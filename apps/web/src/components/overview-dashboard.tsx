"use client";

import { useEffect, useMemo, useState } from "react";

import { ActivityChart } from "@/components/ui/charts";
import {
  CopyId,
  DataTable,
  Empty,
  ErrorState,
  Kpi,
  LoadingState,
  Notice,
  MetaList,
  PageHeader,
  Panel,
  ScopeBar,
  SectionTitle,
  StatusPill,
  Td,
  TechnicalDetails,
  Th,
} from "@/components/ui/ops";
import { ApiError, fetchOverview, getJson } from "@/lib/api";
import type { HealthReady, IngestionStats, RecentEvent, RecentEventsResponse, WarehouseStatus } from "@/lib/types";
import { formatAge, formatCount, formatMoney, formatUtc } from "@/lib/utils";

const REFRESH_MS = 5000;

type QualityRun = {
  run_id: string;
  status: string;
  warehouse_publication_id: string;
  failure_count: number;
  injected: string | null;
  finished_at: string | null;
};

type Snapshot = {
  ready: HealthReady;
  stats: IngestionStats;
  recent: RecentEventsResponse;
  warehouse: WarehouseStatus | null;
  warehouseError: string | null;
  quality: QualityRun | null;
  lag: { available: boolean; total: number | null; detail: string } | null;
  serving: string | null;
  fetchedAt: string;
};

export function OverviewDashboard() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [initialError, setInitialError] = useState<string | null>(null);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [sort, setSort] = useState<"ingested" | "disposition">("ingested");

  useEffect(() => {
    let cancelled = false;
    async function load(isInitial: boolean) {
      try {
        const [ready, stats, recent] = await fetchOverview();
        let warehouse: WarehouseStatus | null = null;
        let warehouseError: string | null = null;
        let quality: QualityRun | null = null;
        let lag: Snapshot["lag"] = null;
        let serving: string | null = null;
        try {
          const monitor = await getJson<{
            ingestion_lag: { available: boolean; total_lag: number | null; detail: string };
            model_serving: string | null;
          }>("/api/v1/monitoring/overview");
          lag = {
            available: monitor.ingestion_lag.available,
            total: monitor.ingestion_lag.total_lag,
            detail: monitor.ingestion_lag.detail,
          };
          serving = monitor.model_serving;
        } catch {
          lag = null;
        }
        try {
          warehouse = await getJson<WarehouseStatus>("/api/v1/warehouse/status");
        } catch (error) {
          warehouseError = error instanceof ApiError ? error.message : "Warehouse status failed.";
        }
        try {
          const runs = await getJson<{ runs: QualityRun[] }>("/api/v1/quality/runs?limit=30");
          const publication = warehouse?.warehouse_publication_id;
          quality =
            runs.runs.find((run) => publication && run.warehouse_publication_id === publication) ??
            null;
        } catch {
          quality = null;
        }
        if (cancelled) {
          return;
        }
        setSnapshot({ ready, stats, recent, warehouse, warehouseError, quality, lag, serving, fetchedAt: new Date().toISOString() });
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

  const events = useMemo(() => {
    const items = [...(snapshot?.recent.items ?? [])];
    items.sort((left, right) => {
      if (sort === "disposition") {
        return left.disposition.localeCompare(right.disposition);
      }
      return right.ingested_at.localeCompare(left.ingested_at);
    });
    return items;
  }, [snapshot, sort]);

  if (loading) {
    return <LoadingState label="Loading persisted ingestion state…" />;
  }
  if (!snapshot) {
    return <ErrorState title="Overview unavailable" detail={initialError} />;
  }

  const { stats, warehouse, quality, lag, serving } = snapshot;
  const activity = stats.recent_activity.map((point) => ({
    label: formatUtc(point.minute).slice(11, 16),
    persisted: point.persisted_events,
  }));
  return (
    <div>
      <PageHeader
        title="Operations overview"
        description="Live ingestion and validated business intelligence"
        aside={<StatusPill tone={warehouse?.ready ? "ok" : "warn"}>{warehouse?.ready ? "Warehouse ready" : "Warehouse not ready"}</StatusPill>}
      />
      {refreshError ? (
        <Notice>The latest refresh failed. Figures below are the previous response. {refreshError}</Notice>
      ) : null}
      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi
          label="Accepted orders"
          value={warehouse?.ready ? formatCount(warehouse.order_count) : "Unavailable"}
          hint="Orders in the published ClickHouse snapshot. This is not the live ingest count."
          footnote={warehouse?.ready ? `Freshness ${formatAge(warehouse.freshness_seconds)}` : warehouse?.reason ?? "Warehouse status was not ready."}
        />
        <Kpi
          label="Ingested events"
          value={formatCount(stats.persisted_events)}
          hint={stats.metric_definitions.persisted_events}
          footnote="PostgreSQL records"
        />
        <Kpi
          label="Data quality"
          value={!warehouse?.ready ? "Unavailable" : quality?.status === "passed" ? "Passed" : quality ? "Failed" : "No evidence"}
          hint="Matched to the serving publication when one exists. A later injected failure on another candidate is not this status."
          footnote={
            quality
              ? `${quality.failure_count} failing checks · ${quality.injected ? `injected ${quality.injected}` : "not injected"}`
              : `Postgres ${snapshot.ready.checks.postgres === "ok" ? "ready" : "not ready"}`
          }
        />
        <Kpi
          label="Consumer lag"
          value={lag?.available ? formatCount(lag.total) : "Unavailable"}
          hint={lag?.detail ?? "Ingestion consumer lag was not returned."}
          footnote="Reported Kafka lag"
        />
      </div>
      <section className="mb-4">
        <SectionTitle
          title="Published revenue"
          detail="One amount per currency. These figures are the snapshot, not a live broker rate."
        />
        {warehouse?.ready && Object.keys(warehouse.accepted_revenue).length > 0 ? (
          <div className="grid gap-3 sm:grid-cols-3">
            {Object.entries(warehouse.accepted_revenue).map(([currency, amount]) => (
              <Kpi key={currency} label={`${currency} revenue`} value={formatMoney(amount, currency)} />
            ))}
          </div>
        ) : (
          <Empty>{snapshot.warehouseError ?? warehouse?.reason ?? "No published revenue is available."}</Empty>
        )}
      </section>
      <div className="grid min-w-0 gap-4 xl:grid-cols-[minmax(0,1.4fr)_minmax(240px,0.7fr)]">
        <Panel title="Ingestion activity" detail="Persisted events · observed minute buckets" action={<StatusPill tone="info">UTC · recorded</StatusPill>}>
          {activity.length === 0 ? (
            <Empty>No events were persisted in the last {stats.activity_window_minutes} minutes.</Empty>
          ) : (
            <ActivityChart points={activity} />
          )}
        </Panel>
        <Panel title="Platform status">
          <MetaList
            rows={[
              { label: "Warehouse", value: warehouse?.ready ? "Validated publication" : warehouse?.reason ?? "Not serving" },
              { label: "Silver", value: warehouse?.silver_publication_id ? "Curated snapshot ready" : "No Silver id" },
              { label: "Forecast serving", value: serving ?? "Not recorded" },
              { label: "Quarantine", value: `${formatCount(stats.quarantined_records)} records` },
            ]}
          />
        </Panel>
      </div>
      <section className="mt-4 min-w-0 rounded-xl border border-[var(--line)] bg-[var(--surface)] p-4">
          <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
            <SectionTitle title="Recent persisted events" detail="Sorted within the returned page." />
            <label className="text-xs text-zinc-400">
              Sort
              <select
                className="ml-2 rounded border border-white/10 bg-[#0e1116] px-2 py-1"
                value={sort}
                onChange={(event) => setSort(event.target.value as "ingested" | "disposition")}
              >
                <option value="ingested">Ingested time</option>
                <option value="disposition">Disposition</option>
              </select>
            </label>
          </div>
          {events.length === 0 ? (
            <Empty>No events have been persisted yet.</Empty>
          ) : (
            <DataTable label="Recent persisted events">
              <thead>
                <tr>
                  <Th>Ingested UTC</Th>
                  <Th>Order</Th>
                  <Th>Disposition</Th>
                  <Th>Amount</Th>
                  <Th>Offset</Th>
                </tr>
              </thead>
              <tbody>
                {events.map((event) => (
                  <EventRow key={event.event_id} event={event} />
                ))}
              </tbody>
            </DataTable>
          )}
        </section>
      <ScopeBar>Scope: persisted events and published snapshots are distinct. Logical orders {formatCount(stats.logical_orders)}.</ScopeBar>
      <div className="mt-4">
        <TechnicalDetails summary="Publication identifiers">
          <p>
            Warehouse <CopyId value={warehouse?.warehouse_publication_id ?? null} />
          </p>
          <p>
            Silver <CopyId value={warehouse?.silver_publication_id ?? null} />
          </p>
          <p className="text-xs text-zinc-500">Quarantined records {formatCount(stats.quarantined_records)}. {stats.metric_definitions.quarantined_records}</p>
        </TechnicalDetails>
      </div>
    </div>
  );
}

function EventRow({ event }: { event: RecentEvent }) {
  return (
    <tr>
      <Td className="font-mono text-xs">{formatUtc(event.ingested_at)}</Td>
      <Td className="font-mono text-xs">{event.order_id ?? "—"}</Td>
      <Td>{event.disposition}</Td>
      <Td className="font-mono text-xs">
        {event.currency && event.total_amount ? formatMoney(event.total_amount, event.currency) : "—"}
      </Td>
      <Td className="font-mono text-xs text-zinc-500">
        {event.kafka_partition}:{event.kafka_offset}
      </Td>
    </tr>
  );
}
