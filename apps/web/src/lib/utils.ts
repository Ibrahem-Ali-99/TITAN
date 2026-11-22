import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatUtc(value: string | null): string {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toISOString().replace("T", " ").replace(/\.\d{3}Z$/, "Z");
}

export function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return new Intl.NumberFormat("en-GB").format(value);
}

export function formatMoney(amount: string | null | undefined, currency: string): string {
  if (!amount) {
    return "—";
  }
  const value = Number(amount);
  if (!Number.isFinite(value)) {
    return `${currency} ${amount}`;
  }
  try {
    return new Intl.NumberFormat("en-GB", {
      style: "currency",
      currency,
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  } catch {
    return `${currency} ${amount}`;
  }
}

export function formatDuration(ms: number | null): string {
  if (ms === null) {
    return "—";
  }
  if (ms < 1000) {
    return `${ms} ms`;
  }
  const seconds = ms / 1000;
  if (seconds < 60) {
    return `${seconds.toFixed(seconds < 10 ? 1 : 0)} s`;
  }
  const minutes = Math.floor(seconds / 60);
  const rest = Math.round(seconds % 60);
  return `${minutes}m ${rest}s`;
}

export function formatAge(seconds: number | null): string {
  if (seconds === null) {
    return "not recorded";
  }
  if (seconds < 60) {
    return `${seconds}s ago`;
  }
  if (seconds < 3600) {
    return `${Math.floor(seconds / 60)}m ago`;
  }
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (seconds < 86400) {
    return `${hours}h ${minutes}m ago`;
  }
  return `${Math.floor(seconds / 86400)}d ${hours % 24}h ago`;
}

const STALE_RUN_MS = 15 * 60 * 1000;

export type RunTone = "ok" | "warn" | "bad" | "neutral";

export function classifyRun(run: {
  state: string;
  started_at: string;
  finished_at?: string | null;
  recovered: boolean;
}): { label: string; tone: RunTone } {
  if (run.state === "succeeded") {
    return { label: "Succeeded", tone: "ok" };
  }
  if (run.state === "failed") {
    return run.recovered
      ? { label: "Recovered", tone: "warn" }
      : { label: "Failed", tone: "bad" };
  }
  if (run.state === "running") {
    if (run.finished_at) {
      return { label: "Status unknown", tone: "neutral" };
    }
    const started = new Date(run.started_at).getTime();
    if (!Number.isFinite(started) || Date.now() - started > STALE_RUN_MS) {
      return { label: "Stale", tone: "warn" };
    }
    return { label: "Running", tone: "neutral" };
  }
  return { label: run.state || "Status unknown", tone: "neutral" };
}

export function hasLaterSuccess<T extends { job_name: string; state: string; started_at: string }>(
  run: T,
  runs: T[],
): boolean {
  const started = new Date(run.started_at).getTime();
  return runs.some(
    (other) =>
      other.job_name === run.job_name &&
      other.state === "succeeded" &&
      new Date(other.started_at).getTime() > started,
  );
}
