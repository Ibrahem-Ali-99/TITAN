---
name: engineering-review
description: Reviews TITAN milestone diffs for architecture, streaming correctness, schema compatibility, security, and executed verification. Use when reviewing a milestone, examining a diff, or before accepting infrastructure, ingestion, API, or dashboard work.
---

# Engineering review

Review the actual diff before accepting a milestone. Do not review from memory of the intended design.

## Required procedure

1. Inspect the actual diff.
2. Review architecture and dependencies.
3. Analyze event consistency and idempotency.
4. Analyze concurrency, failure, replay, and restart behavior.
5. Review schema compatibility and security.
6. Execute relevant validation commands.
7. Classify findings as Critical, Major, Minor, or Suggestion.
8. Fix verified Critical and Major defects before accepting a milestone.
9. Distinguish executed tests from unverified assumptions.

## Finding classes

- **Critical**: data loss, duplicate persistence, an offset commit before durability, secret exposure, or a broken acceptance path.
- **Major**: a realistic local failure that drops, double-counts, or misreports operational data.
- **Minor**: a localized defect with bounded impact.
- **Suggestion**: an improvement that is not required for the milestone.

## Report

For each finding include severity, location, evidence, and whether it was fixed.
State which commands ran and their results. Label anything not executed as unverified.
