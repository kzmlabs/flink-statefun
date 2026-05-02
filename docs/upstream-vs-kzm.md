# Differences from Apache StateFun

Apache Stateful Functions 3.4.0 (October 2024) is the last upstream release, targeting Flink 1.16 and Java 11. Kzmlabs StateFun continues the project against the modern Flink line.

## Core differences

| Aspect | Apache StateFun 3.4.0 | Kzmlabs StateFun KZM-3.1 |
|---|---|---|
| Flink runtime | 1.16.2 | **2.2.0** |
| Java baseline | 11 | **21** |
| Maven group | `org.apache.flink` | `io.github.kzmlabs.flinkstatefun` |
| Kinesis I/O | Built for Flink 1.x Kinesis source | **Restored** on Flink 2.x's `KinesisStreamsSource`/`KinesisStreamsSink` |
| Active CI | Inactive after 3.4.0 | Ongoing — Dependabot, CodeQL, Scorecard, Trivy |
| K8s deployment | Examples in docs | **K8s-native E2E gate** via Flink Operator + LocalStack |
| Docker image | `apache/flink-statefun:3.4.0` (Flink 1.16 base) | `ghcr.io/kzmlabs/flink-statefun:3.4.0-KZM-3.1` (Flink 2.2 base) |

## Migrating user code

Most user code is binary-compatible. The minimum change is the Maven coordinate:

```diff
 <dependency>
-  <groupId>org.apache.flink</groupId>
+  <groupId>io.github.kzmlabs.flinkstatefun</groupId>
   <artifactId>statefun-sdk-java</artifactId>
-  <version>3.4.0</version>
+  <version>3.4.0-KZM-3.1</version>
 </dependency>
```

Function code, `module.yaml`, ingress/egress definitions, and the request-reply HTTP wire format are unchanged.

## Migrating runtime / deployment

If you deploy StateFun via Docker images or the Flink Operator, the runtime image was rebased on Flink 2.2. Notable considerations:

- **Flink configuration keys** — Flink 2.x renamed some configuration keys (e.g. `state.backend` short forms now require fully-qualified `state.backend.type`). Update your `module.yaml`/operator config accordingly.
- **Restart strategy** — Flink 2.x restart strategy uses `execution.restart-strategy.*` prefix.
- **Kinesis routing** — for Flink 2.x's `KinesisStreamsSource`, the `KinesisDeserializationSchema.deserialize` callback now receives the **stream ARN** (not the short name) as the `stream` argument. The Kzmlabs SDK handles this transparently when using `RoutableKinesisIngressSpec`.

See [`guides/kinesis-io.md`](guides/kinesis-io.md) for the full Kinesis configuration model.

## What is *not* different

- The function-as-actor programming model
- The `module.yaml` schema (with the small Kinesis routing change noted above)
- The HTTP request-reply wire protocol — interoperable with the upstream Apache StateFun Python/JS/Go SDKs

## Why the fork exists

Upstream Apache StateFun has not received releases since October 2024. As Flink 2.x ships and JDK 11 leaves the active LTS support window, downstream users are left choosing between staying on an old Flink line or vendoring their own runtime patches. Kzmlabs StateFun is the public, actively maintained continuation — same code, modern dependencies, no vendor lock-in.
