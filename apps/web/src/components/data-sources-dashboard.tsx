"use client";

import { FormEvent, useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import { DataTable, Empty, ErrorState, LoadingState, MetaList, Notice, PageHeader, Panel, ScopeBar, StatusBadge, StatusPill, Td, Th } from "@/components/ui/ops";
import { API_BASE_URL, ApiError, getJson } from "@/lib/api";
import type { CsvImportList, IngestionStats, SourceSubmissionList } from "@/lib/types";
import { formatCount, formatUtc } from "@/lib/utils";

type SourceTab = "CSV upload" | "REST ingestion" | "Import history";

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
  const [tab, setTab] = useState<SourceTab>("CSV upload");
  const [fileName, setFileName] = useState<string | null>(null);

  async function refresh() {
    const [nextSubmissions, nextImports, nextStats] = await Promise.all([
      getJson<SourceSubmissionList>("/api/v1/ingestion/submissions?limit=20"),
      getJson<CsvImportList>("/api/v1/ingestion/imports?limit=20"),
      getJson<IngestionStats>("/api/v1/ingestion/stats"),
    ]);
    setSubmissions(nextSubmissions); setImports(nextImports); setStats(nextStats);
  }

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try { await refresh(); if (!cancelled) setError(null); }
      catch (caught) { if (!cancelled) setError(caught instanceof ApiError ? caught.message : "The API request failed."); }
      finally { if (!cancelled) setLoading(false); }
    }
    void load();
    const timer = window.setInterval(() => void load(), 5000);
    return () => { cancelled = true; window.clearInterval(timer); };
  }, []);

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const formElement = event.currentTarget;
    setUploadMessage(null);
    let response: Response;
    let text: string;
    try { response = await fetch(`${API_BASE_URL}/api/v1/ingestion/imports`, { method: "POST", body: new FormData(formElement) }); text = await response.text(); }
    catch { setUploadMessage("The upload result could not be confirmed. Check import history before retrying."); return; }
    if (response.status === 401 || response.status === 403) { setUploadMessage(response.status === 401 ? "Sign in to upload a CSV." : "CSV upload requires the operator or admin role."); return; }
    if (response.status === 502 || response.status === 504) { setUploadMessage("The upload result could not be confirmed. Check import history before retrying."); return; }
    setUploadMessage(response.ok ? "Upload accepted. Kafka publication continues after this response; downstream persistence is separate." : `Import rejected (${response.status}): ${text}`);
    if (response.ok) { formElement.reset(); setFileName(null); try { await refresh(); } catch (caught) { setError(caught instanceof ApiError ? caught.message : "The API request failed."); } }
  }

  async function showErrors(importId: string) {
    if (openImport === importId) { setOpenImport(null); return; }
    setOpenImport(importId); setImportErrors([]); setImportError(null);
    try { const body = await getJson<{ rows: { row_number: number; status: string; error_detail: string | null }[] }>(`/api/v1/ingestion/imports/${importId}`); setImportErrors(body.rows.filter((row) => row.status !== "published" && row.status !== "publishing")); }
    catch (caught) { setImportError(caught instanceof ApiError ? caught.message : "The import detail request failed."); }
  }

  async function resume(importId: string) {
    setResumeMessage(null);
    try {
      const response = await fetch(`${API_BASE_URL}/api/v1/ingestion/imports/${importId}/publish`, { method: "POST" });
      if (response.status === 401 || response.status === 403) { setResumeMessage("An admin session or API client with the admin token is required to resume publishing."); return; }
      if (response.status === 502 || response.status === 504) { setResumeMessage("The resume result could not be confirmed. Check import status before retrying."); return; }
      setResumeMessage(response.ok ? "Resume accepted. Rows still marked publishing will be sent to Kafka." : `Resume failed (${response.status}): ${await response.text()}`);
    } catch { setResumeMessage("The resume result could not be confirmed. Check import status before retrying."); }
  }

  if (loading) return <LoadingState label="Loading source submissions…" />;
  if (error && !submissions) return <ErrorState title="Data sources unavailable" detail={error} />;

  return (
    <div>
      <PageHeader title="Data sources" description="Unified event entry points with observable submission state" aside={<StatusPill tone="info">3 sources</StatusPill>} />
      {error && submissions ? <Notice>The latest refresh failed. Figures below are the previous response. {error}</Notice> : null}
      <section className="source-cards" aria-label="Ingestion sources">
        <article className="source-card card"><div className="source-icon">↗</div><div><h3>REST API</h3><span>JSON event ingestion</span><div className="source-metric"><b>{formatCount(stats?.rest_events_published)}</b><span>broker acknowledgements</span></div></div><StatusPill tone="ok">available</StatusPill></article>
        <article className="source-card card"><div className="source-icon">▤</div><div><h3>CSV imports</h3><span>Validated bounded upload</span><div className="source-metric"><b>{imports?.items.length ?? 0}</b><span>recorded imports</span></div></div><StatusPill tone="info">500 rows · 1 MiB</StatusPill></article>
        <article className="source-card card"><div className="source-icon">✦</div><div><h3>Synthetic producer</h3><span>Reproducible commerce events</span><div className="source-metric"><b>{formatCount(stats?.persisted_events)}</b><span>persisted records observed</span></div></div><StatusPill tone="info">CLI profile</StatusPill></article>
      </section>
      <ScopeBar>A Kafka acknowledgement is not a PostgreSQL persistence guarantee. CSV upload and resume actions remain protected by the API.</ScopeBar>
      <div className="tabs" role="tablist" aria-label="Data source tools">{(["CSV upload", "REST ingestion", "Import history"] as SourceTab[]).map((item) => <button key={item} type="button" role="tab" aria-selected={tab === item} className={tab === item ? "active" : ""} onClick={() => setTab(item)}>{item}</button>)}</div>

      {tab === "CSV upload" ? <div className="upload-layout">
        <Panel title="Upload a CSV" detail="The API validates rows before publication">
          <form onSubmit={onSubmit} className="p-4">
            <label htmlFor="csv-file" className="dropzone"><span className="drop-icon">⇧</span><strong>{fileName ?? "Choose a CSV file"}</strong><small>Drag and drop is supported by the browser label · maximum 500 rows or 1 MiB</small><span className="button">Browse files</span></label>
            <input id="csv-file" className="sr-only" name="file" type="file" accept=".csv,text/csv" required aria-label="CSV file" onChange={(event) => setFileName(event.target.files?.[0]?.name ?? null)} />
            <div className="upload-rules"><span>Validation errors are retained per row</span><span>Publication is asynchronous</span></div>
            <Button type="submit" className="primary mt-4">Upload CSV</Button>
            <div role="status">{uploadMessage ? <p className="mt-3 text-xs text-[var(--secondary)]">{uploadMessage}</p> : null}</div>
          </form>
        </Panel>
        <Panel title="Latest import status" detail="Observed results from the ingestion API">
          {imports?.items[0] ? <div className="p-4"><div className="flex items-center justify-between"><strong className="font-mono text-sm">{imports.items[0].filename}</strong><StatusBadge tone={imports.items[0].status === "completed" ? "ok" : "neutral"}>{imports.items[0].status}</StatusBadge></div><MetaList rows={[{ label: "Received UTC", value: formatUtc(imports.items[0].received_at) }, { label: "Published rows", value: imports.items[0].accepted_rows }, { label: "Rejected rows", value: imports.items[0].rejected_rows }, { label: "Broker failures", value: imports.items[0].publish_failed_rows }]} /></div> : <Empty>No CSV imports have been recorded.</Empty>}
        </Panel>
      </div> : null}

      {tab === "REST ingestion" ? <div className="two-col"><Panel title="REST ingestion" detail="The live API contract is the source of truth"><div className="p-4"><pre className="json-preview">POST /api/v1/ingestion/events{"\n"}Content-Type: application/json{"\n\n"}{"{ event_type, occurred_at, payload }"}</pre><p className="mt-4 text-sm text-[var(--secondary)]">Broker acknowledgement is recorded in submissions. Authentication and role policy are enforced by the API.</p></div></Panel><Panel title="Submission lifecycle"><MetaList rows={[{ label: "Accepted", value: "Acknowledged by Kafka" }, { label: "Persistence", value: "Asynchronous consumer" }, { label: "Rejected payload", value: "Retained with detail" }, { label: "Administrative actions", value: "Protected route" }]} /></Panel></div> : null}

      {tab === "Import history" ? <div className="space-y-4"><Panel title="Recent submissions" detail="REST events acknowledged by the broker">{submissions?.items.length ? <SubmissionTable submissions={submissions} /> : <Empty>No REST submissions have been recorded.</Empty>}</Panel><Panel title="CSV imports" detail="Errors and resume actions are shown from the live import records"><div role="status">{resumeMessage ? <p className="mb-3 text-xs text-amber-100">{resumeMessage}</p> : null}</div>{imports?.items.length ? <ImportTable imports={imports} openImport={openImport} showErrors={showErrors} resume={resume} /> : <Empty>No CSV imports have been recorded.</Empty>}{openImport ? <ImportErrors error={importError} rows={importErrors} /> : null}</Panel></div> : null}
    </div>
  );
}

function SubmissionTable({ submissions }: { submissions: SourceSubmissionList }) {
  return <DataTable label="Source submissions"><thead><tr><Th>Received UTC</Th><Th>Channel</Th><Th>Status</Th><Th>Event</Th></tr></thead><tbody>{submissions.items.map((item) => <tr key={item.id}><Td className="font-mono text-xs">{formatUtc(item.received_at)}</Td><Td>{item.channel}</Td><Td><StatusBadge tone={item.status === "published" || item.status === "published_to_kafka" ? "ok" : item.status.includes("fail") || item.status === "rejected" ? "bad" : "neutral"}>{item.status}</StatusBadge></Td><Td className="font-mono text-xs">{item.event_type ?? "—"}{item.detail ? <div className="mt-1 font-sans text-xs text-[var(--muted)]">{item.detail}</div> : null}</Td></tr>)}</tbody></DataTable>;
}

function ImportTable({ imports, openImport, showErrors, resume }: { imports: CsvImportList; openImport: string | null; showErrors: (id: string) => void; resume: (id: string) => void }) {
  return <DataTable label="CSV imports"><thead><tr><Th>Received UTC</Th><Th>File</Th><Th>Status</Th><Th>Published</Th><Th>Rejected</Th><Th>Broker failures</Th><Th>Actions</Th></tr></thead><tbody>{imports.items.map((item) => <tr key={item.import_id}><Td className="font-mono text-xs">{formatUtc(item.received_at)}</Td><Td className="font-mono text-xs">{item.filename}</Td><Td>{item.status}</Td><Td>{item.accepted_rows}</Td><Td>{item.rejected_rows}</Td><Td>{item.publish_failed_rows}</Td><Td><div className="flex gap-3"><button type="button" className="text-xs text-[var(--accent)]" onClick={() => void showErrors(item.import_id)}>{openImport === item.import_id ? "Hide" : "Errors"}</button>{item.status !== "completed" ? <button type="button" className="text-xs text-[var(--accent)]" onClick={() => void resume(item.import_id)}>Resume</button> : null}</div></Td></tr>)}</tbody></DataTable>;
}

function ImportErrors({ error, rows }: { error: string | null; rows: { row_number: number; status: string; error_detail: string | null }[] }) {
  if (error) return <p className="mt-3 text-xs text-red-200">{error}</p>;
  if (!rows.length) return <p className="mt-3 text-xs text-[var(--muted)]">No rejected or failed rows are stored for this import.</p>;
  return <div className="mt-3"><DataTable label="CSV row errors"><thead><tr><Th>Row</Th><Th>Status</Th><Th>Validation or broker detail</Th></tr></thead><tbody>{rows.map((row) => <tr key={row.row_number}><Td>{row.row_number}</Td><Td>{row.status}</Td><Td>{row.error_detail ?? "—"}</Td></tr>)}</tbody></DataTable></div>;
}
