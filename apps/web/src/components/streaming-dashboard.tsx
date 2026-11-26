"use client";

import { useEffect, useMemo, useState } from "react";

import { ActivityChart } from "@/components/ui/charts";
import { DataTable, Empty, ErrorState, LoadingState, MetaList, Notice, PageHeader, Panel, ScopeBar, StatusBadge, StatusPill, Td, TechnicalDetails, Th } from "@/components/ui/ops";
import { ApiError, getJson } from "@/lib/api";
import type { IngestionStats, QuarantineList, RecentEvent, RecentEventsResponse, ReplayList } from "@/lib/types";
import { formatCount, formatUtc } from "@/lib/utils";

export function StreamingDashboard() {
  const [stats, setStats] = useState<IngestionStats | null>(null);
  const [events, setEvents] = useState<RecentEventsResponse | null>(null);
  const [quarantine, setQuarantine] = useState<QuarantineList | null>(null);
  const [replays, setReplays] = useState<ReplayList | null>(null);
  const [lag, setLag] = useState<{ available: boolean; total_lag: number | null; detail: string } | null>(null);
  const [query, setQuery] = useState("");
  const [eventType, setEventType] = useState("all");
  const [page, setPage] = useState(1);
  const [selectedEvent, setSelectedEvent] = useState<RecentEvent | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [nextStats, nextEvents, nextQuarantine, nextReplays, nextMonitoring] = await Promise.all([
          getJson<IngestionStats>("/api/v1/ingestion/stats"),
          getJson<RecentEventsResponse>("/api/v1/events/recent?limit=100"),
          getJson<QuarantineList>("/api/v1/ingestion/quarantine?limit=50"),
          getJson<ReplayList>("/api/v1/ingestion/replays?limit=20"),
          getJson<{ ingestion_lag: { available: boolean; total_lag: number | null; detail: string } }>("/api/v1/monitoring/overview").catch(() => null),
        ]);
        if (cancelled) return;
        setStats(nextStats); setEvents(nextEvents); setQuarantine(nextQuarantine); setReplays(nextReplays); setLag(nextMonitoring?.ingestion_lag ?? null); setError(null);
      } catch (caught) {
        if (!cancelled) setError(caught instanceof ApiError ? caught.message : "The API request failed.");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    void load();
    const timer = window.setInterval(() => void load(), 5000);
    return () => { cancelled = true; window.clearInterval(timer); };
  }, []);

  const eventTypes = useMemo(() => ["all", ...new Set((events?.items ?? []).map((item) => item.event_type))], [events]);
  const visibleEvents = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return (events?.items ?? []).filter((item) => {
      const matchesType = eventType === "all" || item.event_type === eventType;
      const haystack = `${item.event_id} ${item.order_id ?? ""} ${item.event_type} ${item.source}`.toLowerCase();
      return matchesType && (!needle || haystack.includes(needle));
    });
  }, [eventType, events, query]);
  const distribution = useMemo(() => {
    const counts = new Map<string, number>();
    for (const item of events?.items ?? []) counts.set(item.event_type, (counts.get(item.event_type) ?? 0) + 1);
    return [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 6);
  }, [events]);
  const pageSize = 20;
  const pageCount = Math.max(1, Math.ceil(visibleEvents.length / pageSize));
  const currentPage = Math.min(page, pageCount);
  const pagedEvents = visibleEvents.slice((currentPage - 1) * pageSize, currentPage * pageSize);

  if (loading) return <LoadingState label="Loading live stream outcomes…" />;
  if (!stats || !events || !quarantine || !replays) return <ErrorState title="Streaming unavailable" detail={error} />;

  const lastActivity = stats.recent_activity.at(-1);
  const inbox = quarantine.items.slice(0, 6);
  return (
    <div>
      <PageHeader title="Event streaming" description="Observed ingestion, delivery and quarantine evidence" aside={<StatusPill tone="info">At least once</StatusPill>} />
      {error ? <Notice>The latest refresh failed. Figures below are the previous response. {error}</Notice> : null}
      <section className="metric-strip card" aria-label="Streaming metrics" tabIndex={0}>
        <div><span>Observed / min</span><b>{lastActivity ? formatCount(lastActivity.persisted_events) : "—"}</b><small>latest bucket</small></div>
        <div><span>Persisted total</span><b>{formatCount(stats.persisted_events)}</b><small>durable events</small></div>
        <div><span>Logical orders</span><b>{formatCount(stats.logical_orders)}</b><small>accepted keys</small></div>
        <div><span>Duplicates</span><b>{formatCount(stats.duplicate_deliveries)}</b><small>idempotent delivery</small></div>
        <div><span>Conflicts</span><b>{formatCount(stats.order_conflicts)}</b><small>excluded from revenue</small></div>
        <div><span>Quarantined</span><b>{formatCount(stats.quarantined_records)}</b><small>traceable records</small></div>
        <div><span>Consumer lag</span><b>{lag?.available ? formatCount(lag.total_lag) : "—"}</b><small>{lag?.available ? "measured records" : "not measured by API"}</small></div>
      </section>

      <div className="two-col mt-4">
        <Panel title="Event throughput" detail="Persisted events in the returned minute buckets" action={<StatusPill tone="info">UTC · observed</StatusPill>}>
          <div className="p-4"><ActivityChart points={stats.recent_activity.map((point) => ({ label: point.minute, persisted: point.persisted_events }))} /></div>
        </Panel>
        <Panel title="Event distribution" detail="Event types in the API's returned recent page">
          <div className="mini-bars p-4" aria-label="Recent event type distribution">
            {distribution.length ? distribution.map(([name, count]) => <div key={name} className="mini-bar-row"><span>{formatEventType(name)}</span><i><b style={{ width: `${Math.max(8, (count / distribution[0][1]) * 100)}%` }} /></i><strong>{count}</strong></div>) : <Empty>No recent events returned.</Empty>}
          </div>
        </Panel>
      </div>

      <Panel title="Recent event stream" detail="Persisted events from the API page (up to 100); search, filter, then paginate" action={<StatusPill tone="info">{visibleEvents.length} matching / {events.items.length} returned</StatusPill>}>
        <div className="table-tools p-4 pb-0"><input className="small-search" aria-label="Search recent events" placeholder="Search event, order or source" value={query} onChange={(event) => { setQuery(event.target.value); setPage(1); }} /><select aria-label="Filter event type" value={eventType} onChange={(event) => { setEventType(event.target.value); setPage(1); }}>{eventTypes.map((item) => <option key={item} value={item}>{item === "all" ? "All event types" : formatEventType(item)}</option>)}</select></div>
        {visibleEvents.length ? <><DataTable label="Recent event stream"><thead><tr><Th>Ingested UTC</Th><Th>Type</Th><Th>Order</Th><Th>Source</Th><Th>Disposition</Th><Th>Amount</Th><Th>Topic / offset</Th></tr></thead><tbody>{pagedEvents.map((item) => <tr key={item.event_id} tabIndex={0} aria-label={`Inspect ${formatEventType(item.event_type)} ${item.event_id}`} onClick={() => setSelectedEvent(item)} onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") setSelectedEvent(item); }}><Td className="font-mono text-xs">{formatUtc(item.ingested_at)}</Td><Td>{formatEventType(item.event_type)}</Td><Td className="font-mono text-xs">{item.order_id ?? "—"}</Td><Td>{item.source}</Td><Td><StatusBadge tone={item.disposition === "accepted" || item.disposition === "persisted" ? "ok" : item.disposition.includes("quarantine") ? "warn" : "neutral"}>{formatDisposition(item.disposition)}</StatusBadge></Td><Td>{item.total_amount ? `${item.total_amount} ${item.currency ?? ""}` : "—"}</Td><Td className="font-mono text-xs">{item.kafka_topic}:{item.kafka_offset}</Td></tr>)}</tbody></DataTable><div className="table-footer" aria-label="Recent event pagination"><span>Showing {(currentPage - 1) * pageSize + 1}–{Math.min(currentPage * pageSize, visibleEvents.length)} of {visibleEvents.length} matching returned records</span><div><button type="button" aria-label="Previous event page" disabled={currentPage === 1} onClick={() => setPage((value) => Math.max(1, value - 1))}>‹</button><span aria-live="polite">Page {currentPage} of {pageCount}</span><button type="button" aria-label="Next event page" disabled={currentPage === pageCount} onClick={() => setPage((value) => Math.min(pageCount, value + 1))}>›</button></div></div></> : <Empty>No persisted events match this filter.</Empty>}
      </Panel>

      <div className="two-col mt-4">
        <Panel title="Quarantine queue" detail="Rejected payloads remain traceable" action={<StatusPill tone={stats.quarantined_records ? "warn" : "ok"}>{formatCount(stats.quarantined_records)} records</StatusPill>}>
          {inbox.length ? <DataTable label="Quarantine queue"><thead><tr><Th>Received UTC</Th><Th>Reason</Th><Th>Stream</Th><Th>Replay</Th></tr></thead><tbody>{inbox.map((item) => <tr key={item.id}><Td className="font-mono text-xs">{formatUtc(item.received_at)}</Td><Td>{formatDisposition(item.reason_code)}</Td><Td>{streamLabel(item.kafka_topic)}</Td><Td>{formatDisposition(item.replay_status)}</Td></tr>)}</tbody></DataTable> : <Empty>No quarantined records are currently returned.</Empty>}
        </Panel>
        <Panel title="Delivery contract" detail="Operational semantics exposed by the API">
          <MetaList rows={[{ label: "Offset commit", value: "After durable storage" }, { label: "Duplicate event id", value: "Idempotent" }, { label: "Conflicting business key", value: "Not counted in revenue" }, { label: "Replay", value: "Audited and protected" }]} />
        </Panel>
      </div>
      <ScopeBar>Consumed outcomes {formatCount(stats.kafka_consumed_events)}. Replay requires authorization. {lag?.available ? `Consumer lag ${formatCount(lag.total_lag)} records, using the Monitoring API view.` : "Consumer lag is not exposed by the current API."}</ScopeBar>
      <TechnicalDetails summary="Quarantine payload evidence and replay history">
        {selectedEvent ? <pre className="json-preview">{JSON.stringify(selectedEvent, null, 2)}</pre> : <Empty>Select a recent event to inspect its stored fields.</Empty>}
        {replays.items.length ? <DataTable label="Replay attempts"><thead><tr><Th>Requested UTC</Th><Th>Outcome</Th><Th>Downstream</Th><Th>Mode</Th><Th>Detail</Th></tr></thead><tbody>{replays.items.map((item) => <tr key={item.id}><Td className="font-mono text-xs">{formatUtc(item.requested_at)}</Td><Td>{item.outcome}</Td><Td>{item.downstream_state}</Td><Td>{item.mode}</Td><Td>{item.detail}</Td></tr>)}</tbody></DataTable> : <Empty>No replay attempts have been recorded.</Empty>}
      </TechnicalDetails>
    </div>
  );
}

function streamLabel(topic: string) {
  const name = topic.toLowerCase();
  if (name.includes("order")) return "Order events";
  if (name.includes("payment")) return "Payment events";
  if (name.includes("inventory")) return "Inventory events";
  return topic;
}

function formatEventType(value: string) {
  const known: Record<string, string> = { order_created: "Order created", payment_completed: "Payment completed", inventory_updated: "Inventory updated" };
  return known[value] ?? formatDisposition(value);
}

function formatDisposition(value: string) {
  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}
