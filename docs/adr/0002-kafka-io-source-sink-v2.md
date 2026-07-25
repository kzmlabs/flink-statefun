---
title: "ADR-0002: Kafka I/O on Source/Sink V2"
description: Migrating StateFun's Kafka ingress and egress off the removed FlinkKafkaConsumer/FlinkKafkaProducer APIs onto Flink 2.x's KafkaSource and KafkaSink.
---

# ADR-0002: Kafka I/O on Source/Sink V2

| | |
|---|---|
| Status | Accepted |
| Date | 2026-04-23 |
| References | KZM-2.0 |

## Context

Flink 2.x removed the legacy `FlinkKafkaConsumer`/`FlinkKafkaProducer` classes (Source/Sink V1) that Apache StateFun's Kafka I/O was built on. Running on Flink 2.2.x meant the Kafka ingress and egress layer had to be rebuilt on the new connector APIs, without breaking the `KafkaIngressSpec`/`KafkaEgressSpec` surface that users and `module.yaml` depend on.

## Decision

Migrate Kafka ingress to `KafkaSource` with a `KafkaRecordDeserializationSchema` delegate, and Kafka egress to `KafkaSink` (Sink V2). Keep `KafkaIngressSpec`, `KafkaEgressSpec`, and the `module.yaml` schema unchanged so this is purely a connector-layer swap, invisible to function code.

## Consequences

- Savepoints taken on upstream Apache StateFun are not restorable on the fork; the new source/sink operators use a different internal state layout than the removed V1 connectors.
- `withKafkaProducerPoolSize` is a silent no-op under `KafkaSink`, which manages its own producer pool internally. Callers relying on this setting see no effect.
- Delivery semantics now map onto Flink's `DeliveryGuarantee` enum; `EXACTLY_ONCE` requires a transactional ID prefix to be configured, unlike the old producer default.
- The StateFun state path itself is byte-identical to upstream: only the connector layer changed. In practice a cross-fork restore still requires dropping the connector operators' state (`allowNonRestoredState`) and re-reading offsets from Kafka, so treat upstream savepoints as non-restorable and plan a reprocessing cutover instead.
- Follow-up: the missing `transactionalIdPrefix` default for `EXACTLY_ONCE` was flagged as a production gap during coverage analysis and should be addressed as a documented requirement or a safer default.
