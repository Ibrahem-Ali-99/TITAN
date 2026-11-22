"use client";

import { useEffect, useMemo, useState } from "react";

import {
  DataTable,
  Empty,
  ErrorState,
  Kpi,
  LoadingState,
  Notice,
  PageHeader,
  SectionTitle,
  Td,
  Th,
} from "@/components/ui/ops";
import { ApiError, getJson } from "@/lib/api";
import type { IngestionStats, QuarantineList, ReplayList } from "@/lib/types";
import { formatCount, formatUtc } from "@/lib/utils";

export function StreamingDashboard() {
  const [stats, setStats] = useState<IngestionStats | null>(null);
  const [quarantine, setQuarantine] = useState<QuarantineList | null>(null);
  const [replays, setReplays] = useState<ReplayList | null>(null);
  const [reason, setReason] = useState("all");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [nextStats, nextQuarantine, nextReplays] = await Promise.all([
          getJson<IngestionStats>("/api/v1/ingestion/stats"),
          getJson<QuarantineList>("/api/v1/ingestion/quarantine?limit=50"),
          getJson<ReplayList>("/api/v1/ingestion/replays?limit=20"),
        ]);
        if (cancelled) {
          return;
        }
        setStats(nextStats);
        setQuarantine(nextQuarantine);
        setReplays(nextReplays);
        setError(null);
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
    const timer = window.setInterval(() => void load(), 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  const reasons = useMemo(
    () => ["all", ...new Set((quarantine?.items ?? []).map((item) => item.reason_code))],
    [quarantine],
  );
  const visible = (quarantine?.items ?? []).filter((item) => reason === "all" || item.reason_code === reason);

  if (loading) {
    return <LoadingState label="Loading ingestion outcomes…" />;
  }
  if (!stats || !quarantine || !replays) {
    return <ErrorState title="Streaming unavailable" detail={error} />;
  }

  return (
    <div>
      <PageHeader
        title="Event streaming"
        description="Delivery, deduplication and quarantine"
      />
      {error ? <Notice>The latest refresh failed. Figures below are the previous response. {error}</Notice> : null}
      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi label="Logical orders" value={formatCount(stats.logical_orders)} hint={stats.metric_definitions.logical_orders} />
        <Kpi label="Duplicate deliveries" value={formatCount(stats.duplicate_deliveries)} hint={stats.metric_definitions.duplicate_deliveries} />
        <Kpi label="Order conflicts" value={formatCount(stats.order_conflicts)} hint={stats.metric_definitions.order_conflicts} />
        <Kpi label="Quarantined" value={formatCount(stats.quarantined_records)} hint={stats.metric_definitions.quarantined_records} />
      </div>
      <section className="mb-4 rounded-xl border border-[var(--line)] bg-[var(--surface)] p-4">
        <div className="mb-3 flex flex-wrap items-end justify-between gap-3">
          <SectionTitle title="Quarantine" detail="Filter applies to the returned page. Malformed payloads stay here with topic, partition, and offset." />
          <label className="text-xs text-zinc-400">
            Reason
            <select
              className="ml-2 rounded border border-white/10 bg-[#0e1116] px-2 py-1"
              value={reason}
              onChange={(event) => setReason(event.target.value)}
            >
              {reasons.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
        </div>
        {visible.length === 0 ? (
          <Empty>No quarantined records match this filter.</Empty>
        ) : (
          <DataTable label="Quarantined records">
            <thead>
              <tr>
                <Th>Received UTC</Th>
                <Th>Reason</Th>
                <Th>Topic</Th>
                <Th>Partition</Th>
                <Th>Offset</Th>
                <Th>Replay</Th>
              </tr>
            </thead>
            <tbody>
              {visible.map((item) => (
                <tr key={item.id}>
                  <Td className="font-mono text-xs">{formatUtc(item.received_at)}</Td>
                  <Td>
                    <div>{item.reason_code}</div>
                    {item.error_detail ? <div className="mt-1 text-xs text-zinc-500">{item.error_detail}</div> : null}
                  </Td>
                  <Td className="font-mono text-xs">{item.kafka_topic}</Td>
                  <Td>{item.kafka_partition}</Td>
                  <Td>{item.kafka_offset}</Td>
                  <Td>{item.replay_status}</Td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
      </section>
      <section className="rounded-xl border border-[var(--line)] bg-[var(--surface)] p-4">
        <SectionTitle
          title="Replay history"
          detail="This page lists attempts. Creating a replay requires the admin token and returns 401 when that token is missing or wrong."
        />
        {replays.items.length === 0 ? (
          <Empty>No replay attempts have been recorded.</Empty>
        ) : (
          <DataTable label="Replay attempts">
            <thead>
              <tr>
                <Th>Requested UTC</Th>
                <Th>Outcome</Th>
                <Th>Downstream</Th>
                <Th>Mode</Th>
                <Th>Detail</Th>
              </tr>
            </thead>
            <tbody>
              {replays.items.map((item) => (
                <tr key={item.id}>
                  <Td className="font-mono text-xs">{formatUtc(item.requested_at)}</Td>
                  <Td>{item.outcome}</Td>
                  <Td>{item.downstream_state}</Td>
                  <Td>{item.mode}</Td>
                  <Td className="text-xs">{item.detail}</Td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
        <p className="mt-3 text-xs text-zinc-500">
          Consumed outcomes {formatCount(stats.kafka_consumed_events)}. {stats.metric_definitions.kafka_consumed_events}
        </p>
      </section>
    </div>
  );
}
