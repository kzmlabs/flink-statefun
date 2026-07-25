---
title: "ADR-0006: Operator 1.15.0 and flinkVersion v2_2"
description: Upgrading the Flink Kubernetes Operator so the FlinkDeployment CR declares the same Flink 2.2 line the runtime image ships.
---

# ADR-0006: Operator 1.15.0 and flinkVersion v2_2

| | |
|---|---|
| Status | Accepted |
| Date | 2026-06-17 |
| Issues/PRs | PR #241 |

## Context

Flink Kubernetes Operator 1.11 supported at most `flinkVersion: v2_0`, while the StateFun runtime image had already moved to Flink 2.2.0. Every `FlinkDeployment` CR in the repository (the K8s E2E gate manifests, docs examples) declared `flinkVersion: v2_0`, a mismatch against the actual runtime running inside the pods. Operator 1.15.0 adds support for `flinkVersion: v2_2`, removing the need for that mismatch.

## Decision

Upgrade the Flink Kubernetes Operator to 1.15.0 and change every `FlinkDeployment` CR's `flinkVersion` from `v2_0` to `v2_2`, matching the Flink 2.2.0 runtime the image actually ships. The existing `flinkConfiguration` keys were audited and kept as-is; they were already valid on the 2.2.0 runtime and did not need to change for this upgrade.

While updating `setup-cluster.sh` for the new operator version, a Helm repo pitfall surfaced: the operator chart repository URL embeds the operator version (`archive.apache.org/dist/flink/flink-kubernetes-operator-<version>/`), so a stale local repo entry combined with `helm repo add ... || true` would silently keep resolving the old chart even after bumping `image.tag`. This produces a chart/CRD/RBAC mismatch against the newer image. The fix is `helm repo add --force-update` paired with a pinned `helm install --version`.

## Consequences

- The CR's declared Flink version matches the runtime that is actually deployed, removing a source of operator-side confusion during troubleshooting.
- Operator-side features gated on Flink 2.x become available (the operator can reason correctly about the job's actual version).
- The Helm repo gotcha is local-environment-specific; CI runners always start from a fresh repo cache and never hit it, but it is worth documenting for anyone reproducing the cluster setup locally.
- Validated with the K8s E2E gate green on both the Kafka and Kinesis suites before merge.
