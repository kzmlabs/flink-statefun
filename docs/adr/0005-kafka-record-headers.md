---
title: "ADR-0005: Kafka record headers"
description: Carrying Kafka record headers end to end through the ingress, function, and egress path without changing the wire protocol.
---

# ADR-0005: Kafka record headers

| | |
|---|---|
| Status | Accepted |
| Date | 2026-07-24 |
| Issues/PRs | PR #264 |

## Context

Kafka records carry headers, key/value metadata pairs alongside the payload, commonly used for trace IDs, correlation IDs, schema hints, retry counters, or routing tags. StateFun had no way to read or write them: `io.statefun.kafka.v1/ingress` dropped headers before they reached a function, and `io.statefun.kafka.v1/egress` had no builder support for attaching them. Users needed this round-trip without a new protocol message and without a cost for topics that never use headers.

## Decision

Headers ride as metadata entries on the existing `TypedValue` protocol message, so the change needs zero changes to `flink-core`: `TypedValue` was already the transport-neutral carrier the runtime uses between ingress, function, and egress. Header values reuse the `has_value` idiom `TypedValue` itself uses, so a Kafka header with a null value is distinguishable from one with an empty value all the way through the pipeline. Every header accessor follows a never-throw rule: a malformed or undecodable header must never fail a live function invocation.

Header forwarding from ingress to function is opt-in per topic via `forwardHeaders` (ingress-level default plus per-topic override, off by default), so topics that never use headers keep an allocation-free hot path. Egress-side header writing has no such flag; it is available unconditionally through the `MessageBuilder`/egress builder API.

SDK additions: `MessageBuilder` header support, `valueAsInt()`-style typed accessors, binary primitive overloads for common types, and `testing.KafkaEgressCapture` for asserting egress headers in unit tests without hand-parsing protobuf.

## Consequences

- Functions can read and propagate headers end to end; the ingress to function to egress round-trip is verified in the K8s E2E suite.
- Existing `module.yaml` files and older SDKs keep working unchanged: `forwardHeaders` is additive and defaults to off, and a runtime with header support stays compatible with remote functions built against older SDKs.
- Polyglot SDKs (Go, JS, Python) can add header support later without a protocol change, since the wire format already carries the metadata.
- Kinesis ingress and egress are unaffected; Kinesis has no record-header concept.
