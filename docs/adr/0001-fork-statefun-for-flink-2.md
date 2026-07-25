---
title: "ADR-0001: Fork Apache StateFun for Flink 2.x"
description: Why StateFun Actors exists as a standalone fork instead of waiting on upstream Apache Stateful Functions.
---

# ADR-0001: Fork Apache StateFun for Flink 2.x

| | |
|---|---|
| Status | Accepted |
| Date | 2026-04-23 |
| References | FLIP-569, KZM-2.0 |

## Context

Apache Stateful Functions had not released since 3.4.0 (October 2024), locked to Flink 1.16 and Java 8-11. `FLIP-569` was discussing retiring the project inside the Apache Flink community. Meanwhile Flink 2.x shipped and Java 11 was leaving the active LTS support window, leaving downstream users to choose between an aging runtime or vendoring their own patches indefinitely. There was no path to run StateFun on Flink 2.2.x without a fork, and no owner willing to carry that work upstream.

## Decision

Fork Apache StateFun as a standalone, actively maintained continuation. Target Flink 2.2.x and Java 21. Publish under the Maven coordinate `io.github.kzmlabs.flinkstatefun` (Maven Central rejects re-publishing under `org.apache.flink` from a non-Apache release process). Preserve wire and state compatibility with upstream where feasible, and keep the `module.yaml` API surface unchanged so existing user code and deployments migrate with minimal changes. Version the fork as `3.4.0-KZM-N.M`, starting from KZM-2.0 (2026-04-23) through the current KZM-3.4 (2026-07-24) line. The repository was later detached from the `apache/flink-statefun` fork network via GitHub support so it is indexable as a standalone project.

## Consequences

- Full ownership of releases, CVE response, and the runtime roadmap, no longer gated on upstream Apache activity.
- Docs site rebranded as "StateFun Actors by Kzmlabs" to distinguish the fork from the dormant upstream project.
- Coordinate change (`io.github.kzmlabs.flinkstatefun`) is a one-time migration cost for adopters; function code, `module.yaml`, and the HTTP wire protocol stay compatible.
- Community outreach (FLIP-569 thread, discoverability work) is an ongoing, self-funded effort rather than inherited from an existing project audience.
- Every subsequent architectural decision (Kafka I/O, Kinesis I/O, K8s E2E gating) follows from this fork existing at all.
