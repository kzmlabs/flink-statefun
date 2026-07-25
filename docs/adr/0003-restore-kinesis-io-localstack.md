---
title: "ADR-0003: Restore Kinesis I/O on Flink 2.x with LocalStack"
description: Rebuilding Kinesis ingress and egress on flink-connector-aws after the KZM-2.0 Kinesis runtime break, and validating it with LocalStack in CI.
---

# ADR-0003: Restore Kinesis I/O on Flink 2.x with LocalStack

| | |
|---|---|
| Status | Accepted |
| Date | 2026-04-29 |
| References | KZM-3.0 |

## Context

KZM-2.0 shipped with Kinesis support at the SDK level only; the runtime path was broken because the Flink 1.x Kinesis connector it depended on does not exist in Flink 2.x. Restoring Kinesis meant rebuilding the ingress and egress on `flink-connector-aws`'s `KinesisStreamsSource`/`KinesisStreamsSink`, and doing so correctly required accounting for behavioral changes in Flink 2.x's connector: it invokes `KinesisDeserializationSchema.deserialize` with the stream ARN, not the short name, and `setStreamArn` (source) versus `setStreamName` (sink) take different identifier forms. Validating this locally and in CI also needed a Kinesis-compatible backend that didn't require real AWS credentials.

## Decision

Rebuild Kinesis I/O on `KinesisStreamsSource`/`KinesisStreamsSink`. Key implementation invariants: routing is ARN-keyed because Flink 2.x passes the ARN as the `stream` argument to the deserializer; `KinesisStreamsSource.setStreamArn()` takes the ARN only while `KinesisStreamsSink.setStreamName()` takes the short name only; `AwsRegion.CustomEndpointAwsRegion` accepts `http://` endpoints (relaxed from HTTPS-only) so LocalStack can be targeted directly. For test infrastructure, replace the previous MinIO-based setup with a single LocalStack 4.1 pod serving both Kinesis and S3, using dummy `test`/`test` credentials, and extend the K8s E2E suite to cover both Kafka and Kinesis pipelines in the same gate.

## Consequences

- Kinesis parity with the pre-fork feature set is restored on Flink 2.x, shipped in KZM-3.0 (2026-04-29).
- One LocalStack pod replaces MinIO for both Kinesis and S3, simplifying E2E test topology.
- E2E coverage now spans both transports (Kafka and Kinesis) from the same `mvn verify` invocation, catching regressions in either ingress path before release.
- Custom Kinesis deserializers written by users must be updated to expect an ARN, not a short stream name, when migrating from Flink 1.x; this is documented in the Kinesis I/O guide as a required migration step.
- The routing model still requires a `streams:` array entry even when `streamArn` is set, since `valueType` and `targets` are read from that entry rather than derived from the ARN alone.
