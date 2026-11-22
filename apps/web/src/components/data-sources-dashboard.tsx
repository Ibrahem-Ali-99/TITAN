"use client";

import { FormEvent, useEffect, useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { API_BASE_URL, ApiError, getJson } from "@/lib/api";
import type { CsvImportList, SourceSubmissionList } from "@/lib/types";
import { formatUtc } from "@/lib/utils";

export function DataSourcesDashboard() {
  const [submissions, setSubmissions] = useState<SourceSubmissionList | null>(null);
  const [imports, setImports] = useState<CsvImportList | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [uploadMessage, setUploadMessage] = useState<string | null>(null);

  async function refresh() {
    const [nextSubmissions, nextImports] = await Promise.all([
      getJson<SourceSubmissionList>("/api/v1/ingestion/submissions?limit=20"),
      getJson<CsvImportList>("/api/v1/ingestion/imports?limit=20"),
    ]);
    setSubmissions(nextSubmissions);
    setImports(nextImports);
  }

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        await refresh();
        if (!cancelled) {
          setError(null);
        }
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

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    setUploadMessage(null);
    const response = await fetch(`${API_BASE_URL}/api/v1/ingestion/imports`, {
      method: "POST",
      body: form,
    });
    const text = await response.text();
    setUploadMessage(response.ok ? text : `Import rejected: ${text}`);
    if (response.ok) {
      event.currentTarget.reset();
      try {
        await refresh();
      } catch (caught) {
        setError(caught instanceof ApiError ? caught.message : "The API request failed.");
      }
    }
  }

  if (loading) {
    return <p className="text-sm text-zinc-400">Loading source submissions…</p>;
  }
  if (error && !submissions) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>API unavailable</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-zinc-300">
          <p>No source counts are shown because the API could not be read.</p>
          <pre className="mt-3 overflow-x-auto text-xs text-red-300">{error}</pre>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Data sources</h1>
        <p className="mt-1 max-w-3xl text-sm text-zinc-400">
          REST and CSV submissions are tracked separately from PostgreSQL persistence. A published
          submission means Kafka acknowledged it. Accepted orders and warehouse revenue are on the
          Overview and Warehouse pages. CSV upload is the local ingestion surface. Resume of a
          stalled import is an admin action and requires the API token.
        </p>
      </div>
      <div className="grid gap-3 md:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle>Synthetic generator</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-zinc-400">
            On-demand CLI. Source name <span className="font-mono">synthetic.event-generator</span>.
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>REST</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-zinc-400">
            <span className="font-mono">POST /api/v1/ingestion/events</span>. Validation failures and
            broker failures are stored as submissions.
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>CSV</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-zinc-400">
            Bounded to 500 data rows and 1 MiB. Invalid rows stay in the import. Valid rows are
            published to Kafka.
          </CardContent>
        </Card>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>CSV import</CardTitle>
        </CardHeader>
        <CardContent>
          <form className="flex flex-wrap items-center gap-3" onSubmit={onSubmit}>
            <input
              className="text-sm text-zinc-200 file:mr-3 file:rounded-md file:border-0 file:bg-zinc-800 file:px-3 file:py-2 file:text-zinc-100"
              name="file"
              type="file"
              accept=".csv,text/csv"
              required
            />
            <Button type="submit">Upload</Button>
          </form>
          {uploadMessage ? (
            <pre className="mt-3 overflow-x-auto text-xs text-zinc-400">{uploadMessage}</pre>
          ) : null}
        </CardContent>
      </Card>
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>Recent REST submissions</CardTitle>
          <Badge>{submissions?.returned ?? 0} returned</Badge>
        </CardHeader>
        <CardContent>
          {submissions && submissions.items.length > 0 ? (
            <SubmissionTable items={submissions.items} />
          ) : (
            <p className="text-sm text-zinc-400">No REST submissions have been recorded.</p>
          )}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>CSV imports</CardTitle>
        </CardHeader>
        <CardContent>
          {imports && imports.items.length > 0 ? (
            <ul className="space-y-2 text-sm">
              {imports.items.map((item) => (
                <li key={item.import_id} className="flex flex-wrap justify-between gap-2 border-t border-zinc-800 py-2">
                  <span>
                    <span className="font-mono text-xs">{item.filename}</span>
                    <span className="ml-2 text-zinc-500">{item.status}</span>
                  </span>
                  <span className="text-xs text-zinc-400">
                    published {item.accepted_rows} · rejected {item.rejected_rows} · broker failures{" "}
                    {item.publish_failed_rows} · {formatUtc(item.received_at)}
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-zinc-400">No CSV imports have been recorded.</p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function SubmissionTable({ items }: { items: SourceSubmissionList["items"] }) {
  return (
    <table className="w-full text-left text-sm">
      <thead className="text-xs tracking-wide text-zinc-500 uppercase">
        <tr>
          <th className="py-2 pr-3 font-medium">Received</th>
          <th className="py-2 pr-3 font-medium">Status</th>
          <th className="py-2 font-medium">Event</th>
        </tr>
      </thead>
      <tbody>
        {items.map((item) => (
          <tr key={item.id} className="border-t border-zinc-800">
            <td className="py-2 pr-3 font-mono text-xs">{formatUtc(item.received_at)}</td>
            <td className="py-2 pr-3 text-xs">{item.status}</td>
            <td className="py-2 font-mono text-xs text-zinc-400">
              {item.event_type ?? "—"} {item.event_id ?? ""}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
