"use client";

import { FormEvent, useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  DataTable,
  Empty,
  ErrorState,
  Kpi,
  LoadingState,
  PageHeader,
  SectionTitle,
  StatusBadge,
  Td,
  Th,
} from "@/components/ui/ops";
import { API_BASE_URL, ApiError, getJson } from "@/lib/api";
import type { CsvImportList, SourceSubmissionList } from "@/lib/types";
import { formatCount, formatUtc } from "@/lib/utils";

export function DataSourcesDashboard() {
  const [submissions, setSubmissions] = useState<SourceSubmissionList | null>(null);
  const [imports, setImports] = useState<CsvImportList | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [uploadMessage, setUploadMessage] = useState<string | null>(null);
  const [token, setToken] = useState("");
  const [resumeMessage, setResumeMessage] = useState<string | null>(null);
  const [openImport, setOpenImport] = useState<string | null>(null);
  const [importErrors, setImportErrors] = useState<{ row_number: number; status: string; error_detail: string | null }[]>([]);
  const [importError, setImportError] = useState<string | null>(null);

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
    const formElement = event.currentTarget;
    const form = new FormData(formElement);
    setUploadMessage(null);
    const response = await fetch(`${API_BASE_URL}/api/v1/ingestion/imports`, { method: "POST", body: form });
    const text = await response.text();
    if (response.status === 401) {
      setUploadMessage("The import was rejected with 401. CSV upload itself does not use the admin token.");
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
    if (!token.trim()) {
      setResumeMessage("Enter the admin token. Resume stays closed without it and was not sent.");
      return;
    }
    const response = await fetch(`${API_BASE_URL}/api/v1/ingestion/imports/${importId}/publish`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token.trim()}` },
    });
    const text = await response.text();
    if (response.status === 401) {
      setResumeMessage("Resume returned 401. The admin token is missing on the API or does not match.");
      return;
    }
    setResumeMessage(response.ok ? "Resume accepted. Rows still marked publishing will be sent to Kafka." : `Resume failed (${response.status}): ${text}`);
  }

  if (loading) {
    return <LoadingState label="Loading source submissions…" />;
  }
  if (error && !submissions) {
    return <ErrorState title="Data sources unavailable" detail={error} />;
  }

  const accepted = imports?.items.reduce((sum, item) => sum + item.accepted_rows, 0) ?? 0;
  const rejected = imports?.items.reduce((sum, item) => sum + item.rejected_rows, 0) ?? 0;

  return (
    <div>
      <PageHeader
        title="Data sources"
        description="A submission status means the API accepted the request and Kafka acknowledged publication. It does not mean the row is already an accepted order in PostgreSQL. CSV is limited to 500 data rows and 1 MiB."
      />
      {error && submissions ? <p className="mb-3 text-sm text-amber-100">The latest refresh failed. {error}</p> : null}
      <div className="mb-4 grid gap-3 md:grid-cols-3">
        <Kpi label="REST submissions returned" value={formatCount(submissions?.returned)} footnote="synthetic.event-generator is the CLI source name" />
        <Kpi label="CSV rows published" value={formatCount(accepted)} footnote="Sum of accepted_rows on the returned imports" />
        <Kpi label="CSV rows rejected" value={formatCount(rejected)} footnote="Validation failures stay on the import" />
      </div>
      <div className="mb-4 grid gap-3 md:grid-cols-3">
        <SourceCard title="Synthetic generator" body="On-demand CLI. Events are labeled synthetic and are not store traffic." />
        <SourceCard title="REST" body="POST /api/v1/ingestion/events. Validation and broker failures are stored as submissions." />
        <SourceCard title="CSV" body="Invalid rows stay in the import. Valid rows are published to Kafka asynchronously." />
      </div>
      <section className="mb-4 rounded-lg border border-white/8 bg-[#151922] p-4">
        <SectionTitle title="CSV upload" />
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
        {uploadMessage ? <p className="mt-3 text-xs text-zinc-300">{uploadMessage}</p> : null}
      </section>
      <section className="mb-4 rounded-lg border border-white/8 bg-[#151922] p-4">
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
                    {item.detail ? <div className="mt-1 font-sans text-xs text-zinc-500">{item.detail}</div> : null}
                  </Td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        ) : (
          <Empty>No REST submissions have been recorded.</Empty>
        )}
      </section>
      <section className="rounded-lg border border-white/8 bg-[#151922] p-4">
        <SectionTitle
          title="CSV imports"
          detail="Resume sends rows still marked publishing. It is an admin call. A missing token is reported as 401 and does not pretend to succeed."
        />
        <label className="mb-3 block text-xs text-zinc-400">
          Admin token
          <input
            className="mt-1 block w-full max-w-sm rounded border border-white/10 bg-[#0e1116] px-2 py-1 text-sm text-zinc-100"
            type="password"
            autoComplete="off"
            value={token}
            onChange={(event) => setToken(event.target.value)}
          />
        </label>
        {resumeMessage ? <p className="mb-3 text-xs text-amber-100">{resumeMessage}</p> : null}
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
              <p className="text-xs text-zinc-500">No rejected or failed rows are stored for this import.</p>
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

function SourceCard({ title, body }: { title: string; body: string }) {
  return (
    <section className="rounded-lg border border-white/8 bg-[#151922] p-4">
      <h2 className="text-sm font-medium text-zinc-100">{title}</h2>
      <p className="mt-2 text-sm leading-6 text-zinc-400">{body}</p>
    </section>
  );
}
