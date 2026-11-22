export const STALE_RUN_MS = 15 * 60 * 1000;

export type IncidentTone = "ok" | "warn" | "bad" | "neutral";

export type IncidentKind =
  | "succeeded"
  | "active_failure"
  | "recovered"
  | "recovery_unknown"
  | "stale"
  | "running"
  | "status_unknown";

export type Execution = {
  id?: number | null;
  job_name: string;
  state: string;
  started_at: string | null;
  finished_at?: string | null;
  scope?: string | null;
};

export type Incident = {
  kind: IncidentKind;
  label: string;
  tone: IncidentTone;
  active: boolean;
};

export type IncidentSummary = {
  incidents: Incident[];
  active: Execution[];
  recovered: number;
  recoveryUnknown: number;
  stale: number;
};

function scopeOf(run: Execution): string | null {
  const value = run.scope;
  if (value === undefined || value === null || value.trim() === "") {
    return null;
  }
  return value;
}

function sameJobAndScope(left: Execution, right: Execution): boolean {
  return left.job_name === right.job_name && scopeOf(left) === scopeOf(right);
}

function timestamp(value: string | null | undefined): number | null {
  if (value === undefined || value === null || value.trim() === "") {
    return null;
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function sameAttempt(left: Execution, right: Execution): boolean {
  if (left.id != null && right.id != null) {
    return left.id === right.id;
  }
  return (
    sameJobAndScope(left, right) &&
    left.state === right.state &&
    left.started_at === right.started_at &&
    (left.finished_at ?? null) === (right.finished_at ?? null)
  );
}

export function isLaterSuccess(run: Execution, other: Execution): boolean {
  if (!sameJobAndScope(run, other) || other.state !== "succeeded" || sameAttempt(run, other)) {
    return false;
  }
  const started = timestamp(run.started_at);
  const otherStarted = timestamp(other.started_at);
  if (started === null || otherStarted === null) {
    return false;
  }
  if (otherStarted > started) {
    return true;
  }
  return otherStarted === started && run.id != null && other.id != null && other.id > run.id;
}

export function matchLatest(run: Execution, jobs: Execution[] | null): Execution | null {
  if (jobs === null) {
    return null;
  }
  return jobs.find((job) => sameJobAndScope(run, job)) ?? null;
}

export function classifyIncident(
  run: Execution,
  page: Execution[],
  latest: Execution | null,
  now?: number | null,
): Incident {
  if (run.state === "succeeded") {
    return { kind: "succeeded", label: "Succeeded", tone: "ok", active: false };
  }
  if (run.state === "failed") {
    const recovered =
      page.some((other) => isLaterSuccess(run, other)) ||
      (latest !== null && isLaterSuccess(run, latest));
    if (recovered) {
      return { kind: "recovered", label: "Recovered", tone: "ok", active: false };
    }
    if (latest !== null && latest.state === "failed" && sameAttempt(run, latest)) {
      return { kind: "active_failure", label: "Failed", tone: "bad", active: true };
    }
    return {
      kind: "recovery_unknown",
      label: "Historical failure · Recovery unknown",
      tone: "warn",
      active: false,
    };
  }
  if (run.state === "running") {
    if (run.finished_at) {
      return { kind: "status_unknown", label: "Status unknown", tone: "neutral", active: false };
    }
    if (now == null) {
      return { kind: "status_unknown", label: "Status unknown", tone: "neutral", active: false };
    }
    const started = timestamp(run.started_at);
    if (started === null || now - started > STALE_RUN_MS) {
      return { kind: "stale", label: "Stale", tone: "warn", active: false };
    }
    return { kind: "running", label: "Running", tone: "neutral", active: false };
  }
  return { kind: "status_unknown", label: "Status unknown", tone: "neutral", active: false };
}

export function summarizeIncidents(
  runs: Execution[],
  jobs: Execution[] | null,
  now?: number | null,
): IncidentSummary {
  const incidents = runs.map((run) => classifyIncident(run, runs, matchLatest(run, jobs), now));
  return {
    incidents,
    active:
      jobs === null
        ? []
        : jobs.filter((job) => classifyIncident(job, [], job, now).active),
    recovered: incidents.filter((item) => item.kind === "recovered").length,
    recoveryUnknown: incidents.filter((item) => item.kind === "recovery_unknown").length,
    stale: incidents.filter((item) => item.kind === "stale").length,
  };
}

export function platformHealth(input: {
  warehouseReady: boolean | null;
  activeFailures: number;
}): { label: string; tone: IncidentTone } {
  if (input.activeFailures > 0) {
    return { label: "Degraded", tone: "bad" };
  }
  if (input.warehouseReady === null) {
    return { label: "Unknown", tone: "neutral" };
  }
  if (input.warehouseReady) {
    return { label: "Ready", tone: "ok" };
  }
  return { label: "Not ready", tone: "warn" };
}
