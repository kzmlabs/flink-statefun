# Kzmlabs StateFun

**Stateful actors on Flink — durable state, exactly-once messages, no database to babysit.**

[![Maven Central](https://img.shields.io/maven-central/v/io.github.kzmlabs.flinkstatefun/statefun-bom?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.kzmlabs.flinkstatefun/statefun-bom)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/kzmlabs/flink-statefun/blob/release/LICENSE)
[![CI](https://github.com/kzmlabs/flink-statefun/actions/workflows/ci.yml/badge.svg?branch=release)](https://github.com/kzmlabs/flink-statefun/actions/workflows/ci.yml?query=branch%3Arelease)

You write a function keyed by a logical id. The runtime gives it per-key durable state, routes messages to it, replays on failure, and connects it to Kafka and Kinesis. Actor programming on top of Flink — without writing a Flink job by hand.

This is the actively maintained continuation of Apache Stateful Functions: same programming model, current Flink, current Java, restored Kinesis I/O, and a real Kubernetes end-to-end gate before every release.

## Why this exists

Apache Stateful Functions stopped shipping in October 2024 at 3.4.0, locked to Flink 1.16 and Java 11. Anyone wanting to run it against modern Flink either pinned old dependencies or vendored their own patches. Kzmlabs StateFun is the public, actively maintained branch — same code, modern stack, no vendor lock-in.

Concrete deltas vs upstream:

- Flink **2.2** runtime (was 1.16)
- Java **21** baseline (was 11)
- Kinesis I/O **restored** on Flink 2.x
- Kubernetes-native **E2E gate** via the Flink Operator + LocalStack on every release
- Active dependency hygiene — Dependabot, OpenSSF Scorecard, CodeQL, Trivy

[See the full delta from upstream →](upstream-vs-kzm.md)

## Quick links

<div class="grid cards" markdown>

- :material-rocket-launch:{ .lg .middle } **[Quickstart](quickstart.md)**

    ---

    Run a StateFun job locally in five minutes — Docker compose, Kafka, a remote function.

- :material-package-variant:{ .lg .middle } **[Install](install.md)**

    ---

    Maven coordinates, BOM, Docker images, version matrix.

- :material-kubernetes:{ .lg .middle } **[K8s deployment](guides/k8s-deployment.md)**

    ---

    Production deployment via the Flink Kubernetes Operator with checkpointed RocksDB state.

- :material-source-branch:{ .lg .middle } **[Release process](release-process.md)**

    ---

    How releases are cut and shipped to Maven Central + GHCR.

</div>

## What you get

- **Per-key durable state** — read and write your function's own state without manually wiring Flink keyed-state primitives.
- **Exactly-once messages** between functions and to/from external systems, riding Flink's checkpointing.
- **Polyglot remote functions** — write functions as HTTP endpoints in any language; the runtime owns state and routing.
- **Deployment flexibility** — embedded in Flink, co-located with the JobManager, or remote HTTP services scaled independently.
- **Actually shipped** — every release is gated on a real K8s end-to-end run with the Flink Operator, Kafka, S3 checkpoints, and the actual remote-function pod.

!!! tip "Coming from Apache StateFun?"

    Most user code keeps working unchanged. The only required change is the Maven coordinate: `org.apache.flink:statefun-*` → `io.github.kzmlabs.flinkstatefun:statefun-*`. Full migration notes in the [upstream comparison](upstream-vs-kzm.md).
