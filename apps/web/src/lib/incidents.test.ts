import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  STALE_RUN_MS,
  classifyIncident,
  platformHealth,
  summarizeIncidents,
  type Execution,
} from "./incidents.ts";

const NOW = Date.parse("2026-11-22T18:00:00Z");

function run(partial: Partial<Execution> & Pick<Execution, "job_name" | "state">): Execution {
  return {
    id: partial.id ?? null,
    started_at: partial.started_at === undefined ? "2026-11-22T17:00:00Z" : partial.started_at,
    finished_at: partial.finished_at ?? null,
    scope: partial.scope ?? null,
    ...partial,
  };
}

describe("incident classification", () => {
  it("marks a failure active only when it is the authoritative latest execution", () => {
    const failure = run({ id: 4, job_name: "warehouse_load", state: "failed", started_at: "2026-11-22T17:40:00Z" });
    const incident = classifyIncident(failure, [failure], failure, NOW);
    assert.equal(incident.kind, "active_failure");
    assert.equal(incident.active, true);
    assert.equal(incident.tone, "bad");
  });

  it("confirms recovery when a later success of the same job and scope is in the page", () => {
    const failure = run({ id: 1, job_name: "warehouse_load", state: "failed", scope: "pub-a" });
    const success = run({
      id: 2,
      job_name: "warehouse_load",
      state: "succeeded",
      scope: "pub-a",
      started_at: "2026-11-22T17:10:00Z",
    });
    const incident = classifyIncident(failure, [failure, success], null, NOW);
    assert.equal(incident.kind, "recovered");
    assert.equal(incident.active, false);
  });

  it("does not treat a truncated page as proof that a failure is active", () => {
    const failure = run({ id: 9, job_name: "warehouse_load", state: "failed" });
    const incident = classifyIncident(failure, [failure], null, NOW);
    assert.equal(incident.kind, "recovery_unknown");
    assert.equal(incident.label, "Historical failure · Recovery unknown");
    assert.equal(incident.active, false);
    assert.notEqual(incident.tone, "bad");
  });

  it("confirms recovery from an authoritative success that is outside the fetched page", () => {
    const failure = run({ id: 3, job_name: "quality_gate", state: "failed", started_at: "2026-11-22T17:00:00Z" });
    const later = run({
      id: 80,
      job_name: "quality_gate",
      state: "succeeded",
      started_at: "2026-11-22T17:30:00Z",
    });
    const incident = classifyIncident(failure, [failure], later, NOW);
    assert.equal(incident.kind, "recovered");
    assert.equal(incident.active, false);
  });

  it("does not accept a success from another job or publication as recovery", () => {
    const failure = run({ id: 1, job_name: "warehouse_load", state: "failed", scope: "pub-a" });
    const otherJob = run({
      id: 2,
      job_name: "clickhouse_load",
      state: "succeeded",
      scope: "pub-a",
      started_at: "2026-11-22T17:20:00Z",
    });
    const otherPublication = run({
      id: 3,
      job_name: "warehouse_load",
      state: "succeeded",
      scope: "pub-b",
      started_at: "2026-11-22T17:20:00Z",
    });
    const page = [failure, otherJob, otherPublication];
    assert.equal(classifyIncident(failure, page, null, NOW).kind, "recovery_unknown");
    assert.equal(classifyIncident(failure, page, otherPublication, NOW).kind, "recovery_unknown");
  });

  it("recovers every earlier failure after one confirmed success of that job", () => {
    const failures = [1, 2, 3].map((id) =>
      run({
        id,
        job_name: "warehouse_models",
        state: "failed",
        started_at: `2026-11-22T17:0${id}:00Z`,
      }),
    );
    const success = run({
      id: 4,
      job_name: "warehouse_models",
      state: "succeeded",
      started_at: "2026-11-22T17:09:00Z",
    });
    const summary = summarizeIncidents([...failures, success], [success], NOW);
    assert.equal(summary.recovered, 3);
    assert.equal(summary.active.length, 0);
    assert.equal(summary.recoveryUnknown, 0);
  });

  it("keeps a long-running record without a live signal stale, and a finished running row unknown", () => {
    const stale = run({
      id: 1,
      job_name: "bronze_stream",
      state: "running",
      started_at: new Date(NOW - STALE_RUN_MS - 1000).toISOString(),
      finished_at: null,
    });
    const finishedMarker = run({
      id: 2,
      job_name: "warehouse_load",
      state: "running",
      started_at: "2026-11-22T17:34:50Z",
      finished_at: "2026-11-22T17:34:50Z",
    });
    const fresh = run({
      id: 3,
      job_name: "bronze_stream",
      state: "running",
      started_at: new Date(NOW - 60_000).toISOString(),
      finished_at: null,
    });
    assert.equal(classifyIncident(stale, [stale], stale, NOW).kind, "stale");
    assert.equal(classifyIncident(stale, [stale], stale, NOW).active, false);
    assert.equal(classifyIncident(finishedMarker, [finishedMarker], finishedMarker, NOW).kind, "status_unknown");
    assert.equal(classifyIncident(fresh, [fresh], fresh, NOW).kind, "running");
  });

  it("does not throw on missing timestamps or malformed history", () => {
    const broken = run({
      id: 1,
      job_name: "warehouse_load",
      state: "failed",
      started_at: "not-a-timestamp",
      finished_at: null,
    });
    const blank = run({ id: 2, job_name: "warehouse_load", state: "", started_at: null });
    const incident = classifyIncident(broken, [broken, blank], null, NOW);
    assert.equal(incident.kind, "recovery_unknown");
    assert.equal(incident.active, false);
    assert.equal(classifyIncident(blank, [blank], null, NOW).kind, "status_unknown");
    const fresh = run({
      id: 3,
      job_name: "bronze_stream",
      state: "running",
      started_at: new Date(NOW - 1000).toISOString(),
      finished_at: null,
    });
    assert.equal(classifyIncident(fresh, [fresh], fresh, null).kind, "status_unknown");
    assert.equal(classifyIncident(fresh, [fresh], fresh, null).active, false);
  });

  it("keeps platform health unknown when readiness is missing, even if historical failures exist", () => {
    const failure = run({ id: 1, job_name: "warehouse_load", state: "failed" });
    const summary = summarizeIncidents([failure], null, NOW);
    assert.equal(summary.recoveryUnknown, 1);
    assert.equal(summary.active.length, 0);
    const health = platformHealth({ warehouseReady: null, activeFailures: summary.active.length });
    assert.equal(health.label, "Unknown");
    assert.notEqual(health.label, "Ready");
    assert.notEqual(health.tone, "bad");
  });

  it("does not change success, in-page recovery, or a ready platform that still has unknown history", () => {
    const success = run({ id: 2, job_name: "quality_gate", state: "succeeded", started_at: "2026-11-22T17:20:00Z" });
    assert.equal(classifyIncident(success, [success], success, NOW).kind, "succeeded");
    const old = run({ id: 1, job_name: "warehouse_load", state: "failed" });
    const summary = summarizeIncidents([old, success], [success], NOW);
    assert.equal(summary.recoveryUnknown, 1);
    assert.equal(summary.active.length, 0);
    const health = platformHealth({ warehouseReady: true, activeFailures: 0 });
    assert.equal(health.label, "Ready");
    assert.equal(health.tone, "ok");
  });
});
