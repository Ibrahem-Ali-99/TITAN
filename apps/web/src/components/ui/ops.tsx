"use client";

import { useState, type ReactNode } from "react";

import { cn } from "@/lib/utils";

type Tone = "ok" | "warn" | "bad" | "info" | "neutral";

const statusTone: Record<Tone, string> = { ok: "success", warn: "warning", bad: "critical", info: "info", neutral: "neutral" };

export function StatusPill({ tone, children }: { tone: Tone; children: ReactNode }) {
  return <span className={cn("status", statusTone[tone])}><i aria-hidden />{children}</span>;
}

export function ScopeBar({ children }: { children: ReactNode }) {
  return <p className="stale-note"><span>{children}</span></p>;
}

export function Panel({ title, detail, action, children, className = "" }: { title: string; detail?: string; action?: ReactNode; children: ReactNode; className?: string }) {
  return <section className={cn("panel card", className)}><div className="panel-head"><div><h3>{title}</h3>{detail ? <p>{detail}</p> : null}</div>{action}</div>{children}</section>;
}

export function MetaList({ rows }: { rows: { label: string; value: ReactNode }[] }) {
  return <div className="meta-list">{rows.map((row) => <div key={row.label}><span>{row.label}</span><b>{row.value}</b></div>)}</div>;
}

export function PageHeader({ title, description, aside }: { title: string; description: string; aside?: ReactNode }) {
  return <div className="page-intro"><div><div className="eyebrow">TITAN / {title.toUpperCase()}</div><h1>{title}</h1><p>{description}</p></div>{aside ? <div className="intro-actions">{aside}</div> : null}</div>;
}

export function Kpi({ label, value, hint, footnote, trend, data, tone = "indigo" }: { label: string; value: string; hint?: string; footnote?: string; trend?: string; data?: number[]; tone?: "indigo" | "cyan" | "green" | "amber" }) {
  const color = tone === "cyan" ? "#22d3ee" : tone === "green" ? "#22c55e" : tone === "amber" ? "#f59e0b" : "#818cf8";
  const max = data ? Math.max(...data) : 0;
  const min = data ? Math.min(...data) : 0;
  const points = data?.map((v, i) => `${i * (100 / Math.max(1, data.length - 1))},${28 - ((v - min) / (max - min || 1)) * 22}`).join(" ");
  return <section className="kpi card"><div className="kpi-top"><span>{label}</span>{hint ? <Hint text={hint} /> : null}</div><div className="kpi-value">{value}</div>{footnote || trend ? <div className="kpi-foot"><span className={trend?.startsWith("+") ? "up" : ""}>{trend}</span><span>{footnote}</span></div> : null}{points ? <svg className="sparkline" viewBox="0 0 100 32" preserveAspectRatio="none" aria-hidden><polyline points={points} fill="none" stroke={color} strokeWidth="2" /><circle cx="100" cy={points.split(" ").at(-1)?.split(",")[1]} r="2.3" fill={color} /></svg> : null}</section>;
}

export function Hint({ text }: { text: string }) {
  return <span className="hint-wrap"><button type="button" className="hint" aria-label={text}>?</button><span role="tooltip">{text}</span></span>;
}

export function StatusBadge({ tone, children }: { tone: Tone; children: ReactNode }) {
  return <span className={cn("status", statusTone[tone])}><i aria-hidden />{children}</span>;
}

export function LoadingState({ label }: { label: string }) {
  return <div className="state-notice loading" aria-busy="true" aria-live="polite"><span className="state-icon" aria-hidden>↻</span><div><b>Loading live state</b><p>{label}</p></div><StatusPill tone="info">Loading</StatusPill></div>;
}

export function ErrorState({ title, detail }: { title: string; detail: string | null }) {
  return <section className="state-notice error"><span className="state-icon" aria-hidden>!</span><div><b>{title}</b><p>No substitute figures are shown.</p>{detail ? <pre>{detail}</pre> : null}</div><StatusPill tone="bad">Unavailable</StatusPill></section>;
}

export function Notice({ children }: { children: ReactNode }) {
  return <p className="prototype-note"><span>⚠ {children}</span></p>;
}

export function Empty({ children }: { children: ReactNode }) {
  return <p className="empty-state">{children}</p>;
}

export function CopyId({ value }: { value: string | null }) {
  const [copied, setCopied] = useState(false);
  if (!value) return <span className="unavailable-text">—</span>;
  return <span className="copy-id"><code>{value}</code><button type="button" onClick={() => { void navigator.clipboard.writeText(value).then(() => { setCopied(true); window.setTimeout(() => setCopied(false), 1500); }); }}>{copied ? "Copied" : "Copy"}</button></span>;
}

export function TechnicalDetails({ summary, children }: { summary: string; children: ReactNode }) {
  return <details className="technical-details"><summary>{summary}</summary><div>{children}</div></details>;
}

export function SectionTitle({ title, detail }: { title: string; detail?: string }) {
  return <div className="section-title"><h2>{title}</h2>{detail ? <p>{detail}</p> : null}</div>;
}

export function DataTable({ children, label }: { children: ReactNode; label: string }) {
  return <div className="table-wrap" role="region" aria-label={label} tabIndex={0}><table>{children}</table></div>;
}

export function Th({ children }: { children: ReactNode }) { return <th>{children}</th>; }
export function Td({ children, className }: { children: ReactNode; className?: string }) { return <td className={className}>{children}</td>; }

export function Segmented<T extends string>({ value, options, onChange, label }: { value: T; options: { id: T; label: string }[]; onChange: (value: T) => void; label: string }) {
  return <div className="tabs" role="group" aria-label={label}>{options.map((option) => <button key={option.id} type="button" aria-pressed={value === option.id} className={value === option.id ? "active" : ""} onClick={() => onChange(option.id)}>{option.label}</button>)}</div>;
}
