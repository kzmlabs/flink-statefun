---
title: Differences from Apache StateFun
description: Migration guide from Apache Stateful Functions 3.4.0 to StateFun Actors on Flink 2.x and Java 21 — coordinate change, runtime configuration, what stays the same.
---

# Differences from Apache StateFun

> Apache Stateful Functions 3.4.0 (October 2024) is the last upstream release, locked to Flink 1.16 and Java 11. StateFun Actors continues the project against the modern Flink line. **Most user code keeps working unchanged**; the differences are at the runtime, dependency, and deployment layer.

## At a glance

| | Apache StateFun 3.4.0 | StateFun Actors KZM-3.4 |
|---|---|---|
| **Flink runtime** | 1.16.2 | **2.2.1** |
| **Java baseline** | 11 | **21** |
| **Maven group** | `org.apache.flink` | `io.github.kzmlabs.flinkstatefun` |
| **Kinesis I/O** | Built for Flink 1.x Kinesis source | **Restored** on Flink 2.x's `KinesisStreamsSource`/`KinesisStreamsSink` |
| **Kafka record headers** | Not supported | **End-to-end round-trip** — `Message#headers()` on ingress, egress builder on write, [opt-in per topic](guides/kafka-headers.md) |
| **Active CI** | Inactive after 3.4.0 | Ongoing — Dependabot, CodeQL, Scorecard, Trivy |
| **K8s deployment** | Examples in docs | **K8s-native E2E gate** via Flink Operator + LocalStack |
| **Docker image** | `apache/flink-statefun:3.4.0` (Flink 1.16) | `ghcr.io/kzmlabs/flink-statefun:3.4.0-KZM-3.3` (Flink 2.2) |
| **Test framework** | JUnit 4 | JUnit Jupiter 5.11 |
| **Release cadence** | Dormant | Active (Maven Central + GHCR) |

## Migrating user code

**Most code is binary-compatible.** The minimum change is the Maven coordinate:

```diff
 <dependency>
-  <groupId>org.apache.flink</groupId>
+  <groupId>io.github.kzmlabs.flinkstatefun</groupId>
   <artifactId>statefun-sdk-java</artifactId>
-  <version>3.4.0</version>
+  <version>3.4.0-KZM-3.3</version>
 </dependency>
```

Function code, `module.yaml`, ingress/egress definitions, `StatefulFunction` implementations, the `Context` and `Message` API, and the HTTP request-reply wire format are **unchanged**.

!!! tip "Use the BOM"

    Importing `statefun-bom` removes individual version pins and keeps the module set + Flink + Java + Protobuf aligned. See the [install guide](install.md).

## Migrating runtime / deployment

If you deploy via Docker images or the Flink Operator, the runtime image was rebased on Flink 2.2. Three things to update:

### 1. Flink configuration keys

Flink 2.x renamed several keys. Update your `module.yaml` / Operator config:

| Old (Flink 1.x) | New (Flink 2.x) |
|---|---|
| `state.backend` | `state.backend.type` |
| `high-availability` | `high-availability.type` |
| `restart-strategy` | `execution.restart-strategy.type` |

Flink 2.x silently ignores the short forms, so the symptom is "the cluster comes up but with default values."

### 2. Restart strategy

Flink 2.x's restart strategy uses the `execution.restart-strategy.*` prefix:

```yaml
flinkConfiguration:
  execution.restart-strategy.type: fixed-delay
  execution.restart-strategy.fixed-delay.attempts: 3
  execution.restart-strategy.fixed-delay.delay: 10s
```

### 3. Kinesis routing (if you use Kinesis I/O)

Flink 2.x's `KinesisStreamsSource` invokes `KinesisDeserializationSchema.deserialize(record, stream, shardId, collector)` with the **stream ARN** as the `stream` argument (Flink 1.x passed the short name).

The Kzmlabs SDK handles this transparently when you use `RoutableKinesisIngressSpec`. Custom deserializers must be updated to expect the ARN.

See the [Kinesis I/O guide](guides/kinesis-io.md) for the full Flink 2.x configuration model.

## What is *not* different

- The function-as-actor programming model
- The `module.yaml` schema (with the small Kinesis routing change noted above)
- The HTTP request-reply wire protocol — interoperable with the upstream Apache StateFun Python/JS/Go SDKs
- State backend semantics (RocksDB, exactly-once via Flink checkpointing)
- Ingress/egress declarations for Kafka

## Why this continuation exists

Upstream Apache StateFun has not received releases since October 2024. As Flink 2.x ships and JDK 11 leaves the active LTS support window, downstream users are left choosing between staying on an old Flink line or vendoring their own runtime patches. StateFun Actors is the public, actively maintained continuation — same code, modern dependencies, no vendor lock-in.

## Next steps

<div class="grid cards" markdown>

- :material-package-variant:{ .lg .middle } &nbsp; **[Install](install.md)** — Maven coordinates, BOM, version matrix.
- :material-rocket-launch:{ .lg .middle } &nbsp; **[Quickstart](quickstart.md)** — verify the migration with a five-minute round-trip.
- :material-aws:{ .lg .middle } &nbsp; **[Kinesis I/O](guides/kinesis-io.md)** — Flink 2.x source/sink specifics (the most material difference).
- :material-kubernetes:{ .lg .middle } &nbsp; **[Kubernetes deployment](guides/k8s-deployment.md)** — Flink 2.x Operator configuration.

</div>
