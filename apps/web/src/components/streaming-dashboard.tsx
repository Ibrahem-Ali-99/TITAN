"use client";

import { useEffect, useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError, getJson } from "@/lib/api";
import type { IngestionStats, QuarantineList, ReplayList } from "@/lib/types";
import { formatUtc } from "@/lib/utils";

export function StreamingDashboard() {
  const [stats, setStats] = useState<IngestionStats | null>(null);
  const [quarantine, setQuarantine] = useState<QuarantineList | null>(null);
  const [replays, setReplays] = useState<ReplayList | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [nextStats, nextQuarantine, nextReplays] = await Promise.all([
          getJson<IngestionStats>("/api/v1/ingestion/stats"),
          getJson<QuarantineList>("/api/v1/ingestion/quarantine?limit=20"),
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

  if (loading) {
    return <p className="text-sm text-zinc-400">Loading ingestion outcomes…</p>;
  }
  if (!stats || !quarantine || !replays) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>API unavailable</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-zinc-300">
          <p>Broker lag and throughput are not estimated when the API cannot be read.</p>
          <pre className="mt-3 overflow-x-auto text-xs text-red-300">{error}</pre>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Streaming</h1>
        <p className="mt-1 max-w-3xl text-sm text-zinc-400">
          These figures are durable PostgreSQL outcomes. Kafka consumer lag, broker throughput, and
          in-flight records are not measured, so they are not shown.
        </p>
      </div>
      {error ? (
        <Card className="border-red-900/80">
          <CardContent className="py-3 text-sm text-red-200">
            The latest refresh failed. The figures below are the previous successful response. {error}
          </CardContent>
        </Card>
      ) : null}
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <Metric label="Consumed outcomes" value={stats.kafka_consumed_events} detail={stats.metric_definitions.kafka_consumed_events} />
        <Metric label="Logical orders" value={stats.logical_orders} detail={stats.metric_definitions.logical_orders} />
        <Metric label="Order conflicts" value={stats.order_conflicts} detail={stats.metric_definitions.order_conflicts} />
        <Metric label="Duplicate deliveries" value={stats.duplicate_deliveries} detail={stats.metric_definitions.duplicate_deliveries} />
      </div>
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>Quarantine</CardTitle>
          <Badge>{quarantine.returned} returned</Badge>
        </CardHeader>
        <CardContent>
          {quarantine.items.length === 0 ? (
            <p className="text-sm text-zinc-400">No quarantined records.</p>
          ) : (
            <ul className="space-y-2 text-sm">
              {quarantine.items.map((item) => (
                <li key={item.id} className="border-t border-zinc-800 py-2">
                  <span className="font-mono text-xs text-zinc-300">{item.reason_code}</span>
                  <span className="ml-2 text-xs text-zinc-500">
                    {item.kafka_topic} {item.kafka_partition}:{item.kafka_offset} · {item.replay_status} ·{" "}
                    {formatUtc(item.received_at)}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>Replay attempts</CardTitle>
          <Badge>{replays.returned} returned</Badge>
        </CardHeader>
        <CardContent>
          {replays.items.length === 0 ? (
            <p className="text-sm text-zinc-400">No replay attempts have been recorded.</p>
          ) : (
            <ul className="space-y-2 text-sm">
              {replays.items.map((item) => (
                <li key={item.id} className="border-t border-zinc-800 py-2">
                  <span className="text-zinc-200">{item.outcome}</span>
                  <span className="ml-2 text-xs text-zinc-500">
                    downstream {item.downstream_state} · {item.mode} · {formatUtc(item.requested_at)}
                  </span>
                  <p className="mt-1 text-xs text-zinc-400">{item.detail}</p>
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function Metric({ label, value, detail }: { label: string; value: number; detail: string }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{label}</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-3xl font-semibold text-zinc-50">{value}</p>
        <p className="mt-2 text-xs leading-5 text-zinc-500">{detail}</p>
      </CardContent>
    </Card>
  );
}
