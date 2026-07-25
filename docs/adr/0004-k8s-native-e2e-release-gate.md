---
title: "ADR-0004: Kubernetes-native E2E as the release gate"
description: Gating every release on a real kind cluster running the Flink Kubernetes Operator, Kafka, and LocalStack, instead of relying on unit tests alone.
---

# ADR-0004: Kubernetes-native E2E as the release gate

| | |
|---|---|
| Status | Accepted |
| Date | 2026-04-24 |
| References | e2e-test.yml; later validated by PR #241, PR #271 |

## Context

Unit and integration tests exercise StateFun's code paths but not the surrounding deployment topology: Flink Operator behavior, Flink 2.x configuration key renames, container image compatibility, and real broker/stream interaction. Configuration-level regressions, such as a restart-strategy key silently falling back to defaults, or an image failing to preload on a containerd store, only surface when the actual Operator and cluster are involved. Relying on unit tests alone let this class of bug reach release.

## Decision

Gate every release on a real `kind` cluster running the Flink Kubernetes Operator (currently 1.15.0, supporting `flinkVersion: v2_2`), Kafka, LocalStack, and a deployed remote-function pod, driven by `mvn verify` in `statefun-e2e-tests`/`statefun-e2e-k8s-native`. The suite runs `StateFunK8sE2E` (`@Tag("kafka")`) and `StateFunKinesisE2E` (`@Tag("kinesis")`) in one invocation. `release.yml` and `docker-release.yml` both gate their publish steps on this suite passing. The CI workflow step passes `-Dskip.teardown=true` so a log-collection step can pull diagnostics from the live cluster before the pipeline's own teardown runs. Locally, the same discipline applies: run the identical `mvn verify` invocation before every push, not just before tagging a release.

## Consequences

- Catches operator- and runtime-level integration bugs that unit tests cannot reach, evidenced by the Flink 2.x restart-strategy key naming issue (PR #271) and container image preload issues on containerd.
- Adds real cost per run: roughly 6 minutes in CI, 10-15 minutes locally, which is accepted as the price of testing against the actual production topology rather than a mock.
- Both release pipelines (Maven Central + GHCR, and Docker-only) depend on the same gate, so a red E2E run blocks all publishing paths, not just one.
- Local-first discipline (run E2E before push) is a process requirement, not just a CI safeguard, since CI capacity and turnaround are shared resources.
- Follow-up: keep the Operator version and `flinkVersion` pinned in the FlinkDeployment CRs in step with the Operator's supported Flink versions as both move forward.
