"use client";

import { useState, type ReactNode } from "react";

import { cn } from "@/lib/utils";

export function PageHeader({
  title,
  description,
  aside,
}: {
  title: string;
  description: string;
  aside?: ReactNode;
}) {
  return (
    <div className="mb-5 flex flex-wrap items-end justify-between gap-3">
      <div className="max-w-3xl">
        <h1 className="text-2xl font-semibold tracking-tight text-zinc-50">{title}</h1>
        <p className="mt-1 text-sm leading-6 text-zinc-400">{description}</p>
      </div>
      {aside ? <div className="text-xs text-zinc-500">{aside}</div> : null}
    </div>
  );
}

export function Kpi({
  label,
  value,
  hint,
  footnote,
}: {
  label: string;
  value: string;
  hint?: string;
  footnote?: string;
}) {
  return (
    <section className="rounded-lg border border-white/8 bg-[#151922] px-4 py-3">
      <div className="flex items-center gap-2">
        <h2 className="text-xs font-medium tracking-wide text-zinc-400 uppercase">{label}</h2>
        {hint ? <Hint text={hint} /> : null}
      </div>
      <p className="mt-2 text-2xl font-semibold tracking-tight text-zinc-50">{value}</p>
      {footnote ? <p className="mt-1 text-xs text-zinc-500">{footnote}</p> : null}
    </section>
  );
}

export function Hint({ text }: { text: string }) {
  return (
    <span className="group relative inline-flex">
      <button
        type="button"
        className="rounded-full border border-zinc-700 px-1.5 text-[10px] text-zinc-400 focus-visible:ring-2 focus-visible:ring-sky-500"
        aria-label={text}
      >
        ?
      </button>
      <span
        role="tooltip"
        className="pointer-events-none absolute top-6 left-0 z-20 hidden w-64 rounded-md border border-zinc-700 bg-zinc-950 p-2 text-xs leading-5 font-normal tracking-normal text-zinc-300 normal-case group-focus-within:block group-hover:block"
      >
        {text}
      </span>
    </span>
  );
}

export function StatusBadge({
  tone,
  children,
}: {
  tone: "ok" | "warn" | "bad" | "info" | "neutral";
  children: ReactNode;
}) {
  const toneClass = {
    ok: "border-emerald-900/80 bg-emerald-950/50 text-emerald-200",
    warn: "border-amber-900/80 bg-amber-950/40 text-amber-100",
    bad: "border-red-900/80 bg-red-950/40 text-red-200",
    info: "border-sky-900/80 bg-sky-950/40 text-sky-100",
    neutral: "border-zinc-700 bg-zinc-900 text-zinc-300",
  }[tone];
  return (
    <span className={cn("inline-flex items-center rounded-md border px-2 py-0.5 text-xs font-medium", toneClass)}>
      {children}
    </span>
  );
}

export function LoadingState({ label }: { label: string }) {
  return (
    <div className="space-y-3" aria-busy="true" aria-live="polite">
      <div className="h-8 w-48 animate-pulse rounded bg-zinc-800" />
      <div className="grid gap-3 sm:grid-cols-3">
        <div className="h-20 animate-pulse rounded-lg bg-zinc-900" />
        <div className="h-20 animate-pulse rounded-lg bg-zinc-900" />
        <div className="h-20 animate-pulse rounded-lg bg-zinc-900" />
      </div>
      <p className="text-sm text-zinc-400">{label}</p>
    </div>
  );
}

export function ErrorState({ title, detail }: { title: string; detail: string | null }) {
  return (
    <section className="rounded-lg border border-red-900/80 bg-red-950/20 p-4">
      <h1 className="text-base font-medium text-red-100">{title}</h1>
      <p className="mt-2 text-sm text-zinc-300">No substitute figures are shown.</p>
      {detail ? <pre className="mt-3 overflow-x-auto text-xs text-red-200">{detail}</pre> : null}
    </section>
  );
}

export function Notice({ children }: { children: ReactNode }) {
  return (
    <p className="mb-4 rounded-md border border-amber-900/70 bg-amber-950/30 px-3 py-2 text-sm text-amber-100">
      {children}
    </p>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <p className="text-sm text-zinc-400">{children}</p>;
}

export function CopyId({ value }: { value: string | null }) {
  const [copied, setCopied] = useState(false);
  if (!value) {
    return <span className="text-zinc-500">—</span>;
  }
  return (
    <span className="inline-flex items-center gap-2">
      <code className="font-mono text-xs text-zinc-300">{value}</code>
      <button
        type="button"
        className="text-xs text-sky-300 underline-offset-2 hover:underline"
        onClick={() => {
          void navigator.clipboard.writeText(value).then(() => {
            setCopied(true);
            window.setTimeout(() => setCopied(false), 1500);
          });
        }}
      >
        {copied ? "Copied" : "Copy"}
      </button>
    </span>
  );
}

export function TechnicalDetails({ summary, children }: { summary: string; children: ReactNode }) {
  return (
    <details className="rounded-lg border border-white/8 bg-[#12151c] px-4 py-3 text-sm text-zinc-300">
      <summary className="cursor-pointer text-sm font-medium text-zinc-200">{summary}</summary>
      <div className="mt-3 space-y-2">{children}</div>
    </details>
  );
}

export function SectionTitle({ title, detail }: { title: string; detail?: string }) {
  return (
    <div className="mb-3">
      <h2 className="text-sm font-medium text-zinc-200">{title}</h2>
      {detail ? <p className="mt-1 text-xs leading-5 text-zinc-500">{detail}</p> : null}
    </div>
  );
}

export function DataTable({ children, label }: { children: ReactNode; label: string }) {
  return (
    <div className="overflow-x-auto" role="region" aria-label={label} tabIndex={0}>
      <table className="w-full min-w-[720px] text-left text-sm">{children}</table>
    </div>
  );
}

export function Th({ children }: { children: ReactNode }) {
  return <th className="py-2 pr-3 text-xs font-medium tracking-wide text-zinc-500 uppercase">{children}</th>;
}

export function Td({ children, className }: { children: ReactNode; className?: string }) {
  return <td className={cn("border-t border-white/8 py-2 pr-3 align-top text-zinc-300", className)}>{children}</td>;
}

export function Segmented<T extends string>({
  value,
  options,
  onChange,
  label,
}: {
  value: T;
  options: { id: T; label: string }[];
  onChange: (value: T) => void;
  label: string;
}) {
  return (
    <div className="inline-flex flex-wrap gap-1 rounded-md border border-white/10 p-1" role="group" aria-label={label}>
      {options.map((option) => (
        <button
          key={option.id}
          type="button"
          aria-pressed={value === option.id}
          className={cn(
            "rounded px-2 py-1 text-xs text-zinc-400",
            value === option.id && "bg-sky-900/70 text-sky-50",
          )}
          onClick={() => onChange(option.id)}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}
