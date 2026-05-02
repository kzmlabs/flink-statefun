# Kzmlabs StateFun

**Actively maintained continuation of Apache Flink Stateful Functions, modernized for Flink 2.x and Java 21.**

[![Maven Central](https://img.shields.io/maven-central/v/io.github.kzmlabs.flinkstatefun/statefun-bom?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.kzmlabs.flinkstatefun/statefun-bom)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/kzmlabs/flink-statefun/blob/release/LICENSE)
[![CI](https://github.com/kzmlabs/flink-statefun/actions/workflows/ci.yml/badge.svg?branch=release)](https://github.com/kzmlabs/flink-statefun/actions/workflows/ci.yml?query=branch%3Arelease)

Build distributed stateful applications and event-driven microservices on Apache Flink with a function-as-actor programming model — durable state, exactly-once messaging, Kafka and Kinesis I/O, and Kubernetes-native deployment via the Flink Kubernetes Operator.

## Why this fork

Apache StateFun's last release (3.4.0, October 2024) targets Flink 1.16. Kzmlabs StateFun continues the project against the modern Flink line and current JDK:

- **Flink 2.2** runtime (vs 1.16 upstream)
- **Java 21** baseline (vs 11)
- **Kinesis I/O** restored on Flink 2.x
- **Kubernetes-native E2E** gate via the Flink Kubernetes Operator and LocalStack
- **Active dependency hygiene** — Dependabot, OpenSSF Scorecard, CodeQL, Trivy

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

## What is Stateful Functions?

Stateful Functions is a programming model for distributed actor-style systems running on Apache Flink. Each function instance is addressed by a logical key, holds durable per-key state, and exchanges messages with other functions or external systems through ingresses (Kafka, Kinesis) and egresses.

Compared to writing a Flink job by hand, StateFun gives you:

- **Per-key state isolation** — your function reads and writes its own state without manually wiring keyed state primitives.
- **Exactly-once messaging** between functions and to/from external systems, leveraging Flink's checkpointing.
- **Polyglot remote functions** — functions can run as HTTP endpoints in any language; the Flink runtime handles state and routing.
- **Deployment flexibility** — embedded with Flink, co-located with the JM, or as remote HTTP services scaled independently.

!!! tip "Coming from Apache StateFun?"

    Read the [upstream comparison](upstream-vs-kzm.md) — most user code keeps working unchanged when bumping the Maven coordinate from `org.apache.flink:statefun-*` to `io.github.kzmlabs.flinkstatefun:statefun-*`.
