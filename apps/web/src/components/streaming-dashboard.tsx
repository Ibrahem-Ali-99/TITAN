"use client";

import { useEffect, useMemo, useState } from "react";

import {
  DataTable,
  Empty,
  ErrorState,
  Kpi,
  LoadingState,
  MetaList,
  Notice,
  PageHeader,
  Panel,
  ScopeBar,
  StatusPill,
  Td,
  TechnicalDetails,
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

  const inbox = visible.slice(0, 6);
  return (
    <div>
      <PageHeader
        title="Event streaming"
        description="Delivery, deduplication and quarantine"
        aside={<StatusPill tone="info">At least once</StatusPill>}
      />
      {error ? <Notice>The latest refresh failed. Figures below are the previous response. {error}</Notice> : null}
      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi label="Events" value={formatCount(stats.persisted_events)} footnote="Durably stored" />
        <Kpi label="Logical orders" value={formatCount(stats.logical_orders)} footnote="Accepted" />
        <Kpi label="Quarantined" value={formatCount(stats.quarantined_records)} footnote="Rejected payloads" />
        <Kpi label="Topics" value="3" footnote="Commerce streams" />
      </div>
      <div className="grid min-w-0 gap-4 xl:grid-cols-[minmax(0,1.4fr)_minmax(240px,0.7fr)]">
        <Panel
          title="Quarantine inbox"
          detail="Durable rejected-event evidence"
          action={<StatusPill tone={stats.quarantined_records > 0 ? "warn" : "ok"}>{formatCount(stats.quarantined_records)} records</StatusPill>}
        >
          <label className="mb-3 block text-xs text-[var(--muted)]">
            Reason
            <select
              className="ml-2 rounded border border-[var(--line)] bg-[var(--background)] px-2 py-1"
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
          {inbox.length === 0 ? (
            <Empty>No quarantined records match this filter.</Empty>
          ) : (
            <div>
              <div className="grid grid-cols-[1.2fr_1fr_auto] gap-3 pb-2 text-[10px] tracking-[0.14em] text-[var(--muted)] uppercase">
                <span>Reason</span>
                <span>Stream</span>
                <span>Replay</span>
              </div>
              {inbox.map((item) => (
                <div key={item.id} className="grid grid-cols-[1.2fr_1fr_auto] gap-3 border-t border-[var(--line)] py-3">
                  <span className="text-sm font-medium">{item.reason_code}</span>
                  <span className="text-sm text-[var(--muted)]">{streamLabel(item.kafka_topic)}</span>
                  <span className="text-xs text-[var(--warn)]">{item.replay_status}</span>
                </div>
              ))}
              <p className="mt-2 text-xs text-[var(--muted)]">Inspect payload, topic, and offset in record details.</p>
            </div>
          )}
        </Panel>
        <Panel title="Delivery contract">
          <MetaList
            rows={[
              { label: "Offset commit", value: "After durable storage" },
              { label: "Duplicate event id", value: "Idempotent" },
              { label: "Conflicting business key", value: "Not counted in revenue" },
              { label: "Replay", value: "Audited and protected" },
            ]}
          />
        </Panel>
      </div>
      <ScopeBar>Rejected payloads remain traceable. Replay requires authorization. Duplicates {formatCount(stats.duplicate_deliveries)}. Conflicts {formatCount(stats.order_conflicts)}.</ScopeBar>
      <div className="mt-4">
        <TechnicalDetails summary="Record details and replay history">
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
                    {item.error_detail ? <div className="mt-1 text-xs text-zinc-400">{item.error_detail}</div> : null}
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
          <h3 className="mt-4 mb-2 text-sm font-medium">Replay history</h3>
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
          <p className="mt-3 text-xs text-[var(--muted)]">
            Consumed outcomes {formatCount(stats.kafka_consumed_events)}. {stats.metric_definitions.kafka_consumed_events}
          </p>
        </TechnicalDetails>
      </div>
    </div>
  );
}

function streamLabel(topic: string) {
  const name = topic.toLowerCase();
  if (name.includes("order")) {
    return "Order events";
  }
  if (name.includes("payment")) {
    return "Payment events";
  }
  if (name.includes("inventory")) {
    return "Inventory events";
  }
  return topic;
}
