---
title: "ADR-0008: Invalid record handling on the Kafka ingress"
description: A configurable policy for malformed Kafka records on the routable ingress, skip by default instead of crashing the whole job.
---

# ADR-0008: Invalid record handling on the Kafka ingress

| | |
|---|---|
| Status | Proposed |
| Date | 2026-07-25 |
| Issues/PRs | Issue #272 |

## Context

The routable Kafka ingress (`io.statefun.kafka.v1/ingress`) has no policy for malformed records today. A null key or a null value (tombstone) throws inside the deserializer and crashes the entire job, every ingress, every function, every topic, not just the one record's topic. An empty key is silently accepted and collapses all such records onto a single function instance, funneling unrelated state together with no log signal. Downstream, an unroutable routing target (a typo in a function address) deploys without error and kills the job on the first message it receives. All of this was verified empirically on a live K8s E2E cluster. In a production topology with 20 ingress topics feeding 20 pipelines, one producer bug on one topic halts all 20.

This compounds the restart-strategy gap fixed in ADR-0007: with the corrected restart keys a poison pill now reaches a terminal `FAILED` state instead of looping forever, but the job still goes down entirely for one bad record.

## Decision

Introduce `invalidRecordHandling` as a per-ingress default with per-topic override, mirroring the existing placement of `forwardHeaders`. It is a camelCase field holding an object with a `type` discriminator, consistent with existing spec conventions like `deliverySemantic`:

- `type: skip` (new default, no yaml required): the record is dropped, a log entry is emitted with full context (topic, partition, offset, timestamp, defect class, error, key, value size), a rate limiter prevents log flooding under sustained garbage (keyed by topic and defect class, so distinct defects always log fully; suppressed repeats surface in periodic summary lines carrying counts and per-partition offset ranges), and a metric counter increments for every record. The job never leaves `RUNNING`. The log level defaults to ERROR and can be lowered per ingress or per topic with `logLevel: warn`.
- `type: fail`: restores the previous behavior, the job crashes on the first invalid record, with pinned exception contracts so existing log-grepping tooling keeps working.
- `type: forward`: the invalid record is delivered to a configured dead-letter function as a normal message, not a special envelope. The payload keeps the topic's configured `valueType` with the original bytes verbatim (`has_value=false` for tombstones). Provenance rides as message metadata under the reserved `statefun.invalid/` prefix (`kafka.topic`, `kafka.partition`, `kafka.offset`, `kafka.timestamp`, `defect`, `error`, `key`). Original record headers pass through unchanged. Any incoming header already using the reserved prefix is stripped so provenance cannot be spoofed by producers. The function instance id is the record key when present, or the literal `"none"` otherwise.

Bind-time validation of routing targets was considered and dropped: with remote functions the runtime cannot verify at bind time that the service actually implements a target, so a static check could only catch namespace-level typos against endpoint patterns. An unroutable target therefore keeps its current behavior (fails at dispatch on first message) and remains documented as a deployment-hygiene concern.

## Consequences

- The default behavior changes from crash-the-job to skip-with-ERROR-log; this is a breaking behavior change that must be called out in release notes.
- Operators who currently alert on job restarts as their signal for bad data need to migrate that alert to the `numInvalidRecordsSkipped` metric instead. Metric naming follows Flink conventions (camelCase `numXxx`, cf. `numLateRecordsDropped`), and the FLIP-33 standard source counter `numRecordsInErrors` increments alongside the policy-specific counters so existing dashboards pick the signal up unchanged.
- `type: forward` gives StateFun a dead-letter path that reuses the existing message and metadata machinery from ADR-0005 (Kafka record headers) with no new protocol additions, and no new runtime machinery beyond the existing ingress-to-function path.
- `type: fail` remains available for teams that prefer the old strict contract, one line of yaml away.
- Unroutable targets (a typo'd namespace in `targets:`) still fail at first message, past the reach of this policy; keeping routing targets correct stays a deploy-review concern.
