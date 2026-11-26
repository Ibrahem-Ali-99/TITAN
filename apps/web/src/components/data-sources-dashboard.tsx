"use client";

import { FormEvent, useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  DataTable,
  Empty,
  ErrorState,
  Kpi,
  LoadingState,
  MetaList,
  PageHeader,
  Panel,
  ScopeBar,
  SectionTitle,
  StatusBadge,
  StatusPill,
  Td,
  Th,
} from "@/components/ui/ops";
import { API_BASE_URL, ApiError, getJson } from "@/lib/api";
import type { CsvImportList, IngestionStats, SourceSubmissionList } from "@/lib/types";
import { formatCount, formatUtc } from "@/lib/utils";

export function DataSourcesDashboard() {
  const [submissions, setSubmissions] = useState<SourceSubmissionList | null>(null);
  const [imports, setImports] = useState<CsvImportList | null>(null);
  const [stats, setStats] = useState<IngestionStats | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [uploadMessage, setUploadMessage] = useState<string | null>(null);
  const [resumeMessage, setResumeMessage] = useState<string | null>(null);
  const [openImport, setOpenImport] = useState<string | null>(null);
  const [importErrors, setImportErrors] = useState<{ row_number: number; status: string; error_detail: string | null }[]>([]);
  const [importError, setImportError] = useState<string | null>(null);

  async function refresh() {
    const [nextSubmissions, nextImports, nextStats] = await Promise.all([
      getJson<SourceSubmissionList>("/api/v1/ingestion/submissions?limit=20"),
      getJson<CsvImportList>("/api/v1/ingestion/imports?limit=20"),
      getJson<IngestionStats>("/api/v1/ingestion/stats"),
    ]);
    setSubmissions(nextSubmissions);
    setImports(nextImports);
    setStats(nextStats);
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
    const formElement = event.currentTarget;
    const form = new FormData(formElement);
    setUploadMessage(null);
    let response: Response;
    let text: string;
    try {
      response = await fetch(`${API_BASE_URL}/api/v1/ingestion/imports`, { method: "POST", body: form });
      text = await response.text();
    } catch {
      setUploadMessage("The upload result could not be confirmed. Check recent imports before retrying.");
      return;
    }
    if (response.status === 401 || response.status === 403) {
      setUploadMessage(response.status === 401 ? "Sign in to upload a CSV." : "CSV upload requires the operator or admin role.");
      return;
    }
    if (response.status === 502 || response.status === 504) {
      setUploadMessage("The upload result could not be confirmed. Check recent imports before retrying.");
      return;
    }
    setUploadMessage(response.ok ? "Upload accepted. Kafka publication continues after this response. Downstream persistence is separate." : `Import rejected (${response.status}): ${text}`);
    if (response.ok) {
      formElement.reset();
      try {
        await refresh();
      } catch (caught) {
        setError(caught instanceof ApiError ? caught.message : "The API request failed.");
      }
    }
  }

  async function showErrors(importId: string) {
    if (openImport === importId) {
      setOpenImport(null);
      return;
    }
    setOpenImport(importId);
    setImportErrors([]);
    setImportError(null);
    try {
      const body = await getJson<{
        rows: { row_number: number; status: string; error_detail: string | null }[];
      }>(`/api/v1/ingestion/imports/${importId}`);
      setImportErrors(body.rows.filter((row) => row.status !== "published" && row.status !== "publishing"));
    } catch (caught) {
      setImportError(caught instanceof ApiError ? caught.message : "The import detail request failed.");
    }
  }

  async function resume(importId: string) {
    setResumeMessage(null);
    try {
      const response = await fetch(`${API_BASE_URL}/api/v1/ingestion/imports/${importId}/publish`, {
        method: "POST",
      });
      if (response.status === 401 || response.status === 403) {
        setResumeMessage("An admin session or an API client with the admin token is required to resume publishing.");
        return;
      }
      if (response.status === 502 || response.status === 504) {
        setResumeMessage("The resume result could not be confirmed. Check the import status before retrying.");
        return;
      }
      setResumeMessage(response.ok ? "Resume accepted. Rows still marked publishing will be sent to Kafka." : `Resume failed (${response.status}): ${await response.text()}`);
    } catch {
      setResumeMessage("The resume result could not be confirmed. Check the import status before retrying.");
    }
  }

  if (loading) {
    return <LoadingState label="Loading source submissions…" />;
  }
  if (error && !submissions) {
    return <ErrorState title="Data sources unavailable" detail={error} />;
  }

  return (
    <div>
      <PageHeader
        title="Data sources"
        description="Unified event entry points"
        aside={<StatusPill tone="info">3 sources</StatusPill>}
      />
      {error && submissions ? <p className="mb-3 text-sm text-amber-100">The latest refresh failed. {error}</p> : null}
      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi label="REST" value={formatCount(stats?.rest_events_published)} footnote="Broker acknowledgement" />
        <Kpi label="CSV limit" value="500 rows" footnote="Maximum 1 MiB" />
        <Kpi label="Generator" value="Ready" footnote="Reproducible events" />
        <Kpi label="Persisted" value={formatCount(stats?.persisted_events)} footnote="Operational records" />
      </div>
      <div className="grid min-w-0 gap-4 xl:grid-cols-[minmax(0,1.4fr)_minmax(240px,0.7fr)]">
        <Panel title="Ingestion sources" detail="Event publishing routes" action={<StatusPill tone="info">3 sources</StatusPill>}>
          <SourceRow title="REST API" body="JSON event ingestion" status={`${formatCount(stats?.rest_events_published)} acknowledged`} />
          <SourceRow title="CSV imports" body="Bounded upload · 500 rows" status={imports?.items.length ? "Imports recorded" : "No imports yet"} />
          <SourceRow title="Synthetic producer" body="Deterministic business events" status="CLI available" />
        </Panel>
        <Panel title="Submission lifecycle">
          <MetaList
            rows={[
              { label: "Accepted", value: "Acknowledged by Kafka" },
              { label: "Persistence", value: "Asynchronous consumer" },
              { label: "Rejected rows", value: "Retain validation errors" },
              { label: "Administrative actions", value: "Admin access required" },
            ]}
          />
        </Panel>
      </div>
      <ScopeBar>A Kafka acknowledgement is not a PostgreSQL persistence guarantee.</ScopeBar>
      <section className="mt-4 rounded-xl border border-[var(--line)] bg-[var(--surface)] p-4">
        <h2 className="mb-3 text-sm font-medium">CSV upload</h2>
        <form className="flex flex-wrap items-center gap-3" onSubmit={onSubmit}>
          <input
            className="text-sm text-zinc-200 file:mr-3 file:rounded-md file:border-0 file:bg-sky-900 file:px-3 file:py-2 file:text-sky-50"
            name="file"
            type="file"
            accept=".csv,text/csv"
            required
            aria-label="CSV file"
          />
          <Button type="submit">Upload</Button>
        </form>
        <div role="status">{uploadMessage ? <p className="mt-3 text-xs text-zinc-300">{uploadMessage}</p> : null}</div>
      </section>
      <section className="mb-4 rounded-xl border border-[var(--line)] bg-[var(--surface)] p-4">
        <SectionTitle title="Recent submissions" />
        {submissions && submissions.items.length > 0 ? (
          <DataTable label="Source submissions">
            <thead>
              <tr>
                <Th>Received UTC</Th>
                <Th>Channel</Th>
                <Th>Status</Th>
                <Th>Event</Th>
              </tr>
            </thead>
            <tbody>
              {submissions.items.map((item) => (
                <tr key={item.id}>
                  <Td className="font-mono text-xs">{formatUtc(item.received_at)}</Td>
                  <Td>{item.channel}</Td>
                  <Td>
                    <StatusBadge tone={item.status === "published" || item.status === "published_to_kafka" ? "ok" : item.status.includes("fail") || item.status === "rejected" ? "bad" : "neutral"}>
                      {item.status}
                    </StatusBadge>
                  </Td>
                  <Td className="font-mono text-xs">
                    {item.event_type ?? "—"}
                    {item.detail ? <div className="mt-1 font-sans text-xs text-zinc-400">{item.detail}</div> : null}
                  </Td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        ) : (
          <Empty>No REST submissions have been recorded.</Empty>
        )}
      </section>
      <section className="rounded-xl border border-[var(--line)] bg-[var(--surface)] p-4">
        <SectionTitle
          title="CSV imports"
          detail="Resume sends rows still marked publishing. In protected staging, this action requires an admin session."
        />
        <div role="status">{resumeMessage ? <p className="mb-3 text-xs text-amber-100">{resumeMessage}</p> : null}</div>
        {imports && imports.items.length > 0 ? (
          <DataTable label="CSV imports">
            <thead>
              <tr>
                <Th>Received UTC</Th>
                <Th>File</Th>
                <Th>Status</Th>
                <Th>Published</Th>
                <Th>Rejected</Th>
                <Th>Broker failures</Th>
                <Th>Rows</Th>
              </tr>
            </thead>
            <tbody>
              {imports.items.map((item) => (
                <tr key={item.import_id}>
                  <Td className="font-mono text-xs">{formatUtc(item.received_at)}</Td>
                  <Td className="font-mono text-xs">{item.filename}</Td>
                  <Td>{item.status}</Td>
                  <Td>{item.accepted_rows}</Td>
                  <Td>{item.rejected_rows}</Td>
                  <Td>{item.publish_failed_rows}</Td>
                  <Td>
                    <div className="flex gap-3">
                      <button type="button" className="text-xs text-sky-300" onClick={() => void showErrors(item.import_id)}>
                        {openImport === item.import_id ? "Hide" : "Errors"}
                      </button>
                      {item.status !== "completed" ? (
                        <button type="button" className="text-xs text-sky-300" onClick={() => void resume(item.import_id)}>
                          Resume
                        </button>
                      ) : null}
                    </div>
                  </Td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        ) : (
          <Empty>No CSV imports have been recorded.</Empty>
        )}
        {openImport ? (
          <div className="mt-3">
            {importError ? <p className="text-xs text-red-200">{importError}</p> : null}
            {!importError && importErrors.length === 0 ? (
              <p className="text-xs text-zinc-400">No rejected or failed rows are stored for this import.</p>
            ) : null}
            {importErrors.length > 0 ? (
              <DataTable label="CSV row errors">
                <thead>
                  <tr>
                    <Th>Row</Th>
                    <Th>Status</Th>
                    <Th>Validation or broker detail</Th>
                  </tr>
                </thead>
                <tbody>
                  {importErrors.map((row) => (
                    <tr key={row.row_number}>
                      <Td>{row.row_number}</Td>
                      <Td>{row.status}</Td>
                      <Td className="text-xs">{row.error_detail ?? "—"}</Td>
                    </tr>
                  ))}
                </tbody>
              </DataTable>
            ) : null}
          </div>
        ) : null}
      </section>
    </div>
  );
}

function SourceRow({ title, body, status }: { title: string; body: string; status: string }) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3 border-t border-[var(--line)] py-3 first:border-t-0">
      <div>
        <p className="text-sm font-medium">{title}</p>
        <p className="text-xs text-[var(--muted)]">{body}</p>
      </div>
      <StatusPill tone="info">{status}</StatusPill>
    </div>
  );
}
