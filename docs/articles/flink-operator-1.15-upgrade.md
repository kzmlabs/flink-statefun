---
title: Upgrading the Apache Flink Kubernetes Operator from 1.11 to 1.15 for Flink 2.2
description: A practical walkthrough of upgrading the Apache Flink Kubernetes Operator from 1.11 to 1.15 to run Flink 2.2 — version compatibility, what changed across releases, the flinkVersion change, a conservative configuration audit, and validation.
---

# Upgrading the Apache Flink Kubernetes Operator from 1.11 to 1.15 for Flink 2.2

*Published 2026-06-17 · by the kzmlabs maintainers*

If you run Flink on Kubernetes through the [Flink Kubernetes Operator][operator-docs], the operator — not your image — decides which Flink version you can declare. You can ship a `flink:2.2.0` image, but if the operator is too old it won't accept `flinkVersion: v2_2`, so you pin down to whatever it knows and run a version mismatch on purpose.

This is what it took to close that gap: moving the operator from **1.11 to 1.15**, switching `flinkVersion` to `v2_2`, and doing it without rewriting config that didn't need to change. The worked example is our own migration of [`kzmlabs/flink-statefun`][our-repo], the Stateful Functions continuation on Flink 2.x, but none of it is StateFun-specific — it applies to any Flink app deployed through the operator.

## The operator gates your Flink version

The operator validates the `flinkVersion` field on a `FlinkDeployment` against the versions it knows how to reconcile. Too old an operator and the newer value isn't recognized, so you pin down and run a mismatch.

Support for each Flink version lands in a specific operator release, and each operator supports a rolling window of recent Flink minors. The releases between 1.11 and 1.15:

| Operator | Released | Supported Flink versions | Notable additions |
|---|---|---|---|
| 1.11 | Mar 2025 | 1.17–1.20, 2.0 | Flink 2.0 (preview); `v1_18+` version-range config |
| 1.12 | Jun 2025 | 1.17–1.20, 2.0 | `config.yaml` support; Flink 2.x config validation |
| 1.13 | Sep 2025 | 1.19, 1.20, 2.0, 2.1 | **Flink 2.1**; structured-YAML `flinkConfiguration` |
| 1.14 | Feb 2026 | 1.19, 1.20, 2.0, 2.1 | `FlinkBlueGreenDeployment` CRD (native blue/green) |
| 1.15 | May 2026 | 1.19, 1.20, 2.0, 2.1, 2.2 | **Flink 2.2**; Logback option; bundled metric reporters |

The *supported* column lists the actively supported (non-deprecated) `flinkVersion` values each release accepts — older versions stay accepted but deprecated. The gating facts: Flink 2.0 needs operator 1.11+, Flink 2.1 needs 1.13+, and Flink 2.2 needs 1.15+.

Before 1.15, a Flink 2.2.0 image had to be declared `flinkVersion: v2_0`. That was the workaround being carried, and the reason for the upgrade.

## What changed between 1.11 and 1.15

Jumping from 1.11 to 1.15 crosses four releases. The changes most relevant to an upgrade:

- **1.12** — the operator emits Kubernetes events when a job enters `FAILED` or restarts unexpectedly, controller errors are reported with more context, and Flink's `config.yaml` format is supported alongside validation of Flink 2.x configuration.
- **1.13** — Flink 2.1 support, plus the option to write `flinkConfiguration` as structured YAML rather than flat strings; also a fix for a job-failure bug when upgrading from 1.12.
- **1.14** — native blue/green deployments through a new `FlinkBlueGreenDeployment` custom resource: zero-downtime upgrades, savepoint-based state hand-off, and rollback. This introduces new cluster-scoped CRDs and RBAC, which a clean Helm chart install picks up automatically.
- **1.15** — Flink 2.2 support, an optional Logback logging framework, metric reporters bundled into the Helm chart, and Kubernetes-native status `Conditions` on `FlinkDeployment`.

## The upgrade

The mechanical part is small. Bump the Helm release:

```bash
helm repo add --force-update flink-operator-repo \
  "https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.15.0/"
helm repo update flink-operator-repo
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-operator --create-namespace --wait --timeout 5m \
  --version "1.15.0" \
  --set image.tag="1.15.0"
```

`--force-update` overwrites a stale version-specific repo URL left by an earlier run, and `--version` pins the chart so a repo/version mismatch fails loudly ("chart not found") instead of silently installing the wrong chart against your new `image.tag`.

Then change the one field this is all about:

```yaml
# before
spec:
  image: ghcr.io/your-org/your-app:flink-2.2.0
  flinkVersion: "v2_0"   # only because the operator capped here

# after
spec:
  image: ghcr.io/your-org/your-app:flink-2.2.0
  flinkVersion: "v2_2"   # now matches the runtime
```

The CRD `apiVersion` is unchanged — operator 1.15 still serves `flink.apache.org/v1beta1`, so there's no group/version rewrite in your manifests.

## Auditing `flinkConfiguration` conservatively

This is where upgrades go sideways. The temptation is to rewrite every `flinkConfiguration` key to match the latest docs. That's usually a mistake.

`flinkVersion` drives the operator's reconciler behaviour, not the runtime's config parsing. Your `flinkConfiguration` map is handed to a Flink 2.2.0 process whether the operator label says `v2_0` or `v2_2`. If the job already ran on a 2.2.0 image, those keys were already being parsed by Flink 2.2, and they worked. Bumping `flinkVersion` doesn't change how the runtime reads them.

That gives a low-risk rule: only remove or rename a key when you can prove it was removed, renamed, or rejected on the target Flink version. Keys that merely look redundant with defaults stay. Anything ambiguous stays and gets a note instead of a deletion.

The asymmetry is the argument. Flink warns on unknown config keys and moves on, so a stale key is cosmetic — but a wrongly deleted key is a behaviour change. In our migration the audit changed exactly one thing of substance: `flinkVersion`. The checkpointing, state-backend, HA, and restart-strategy blocks already ran clean on 2.2.0, so they stayed — verified, not assumed.

## Logging: the operator pod won't emit JSON without a custom image

The JobManager and TaskManager can emit JSON cleanly, because they run your application image — you ship the encoder (for example `logstash-logback-encoder`) and point `spec.logConfiguration` at a JSON `logback-console.xml`. The operator upgrade doesn't touch that.

The operator pod is different. Operator 1.15 can run on Logback through a new `logging.framework=logback` Helm value, but its base image ships Logback 1.2.x with no JSON encoder on the classpath, and Logback's built-in `JsonEncoder` only arrived in 1.5.x. JSON logs from the operator pod would mean building a custom operator image to add the encoder.

We didn't. The operator is an infrastructure controller, not application output, and a custom image to reformat its logs is maintenance you pay again at every operator bump. We left it on the Log4j2 default and kept the application logs as JSON. If your log pipeline genuinely requires JSON from every pod, the custom-image route exists.

## Verifying the upgrade

Two checks, in order.

The operator is healthy when the pod is fully ready and the chart and app versions agree — the latter being the detail that catches version skew:

```bash
kubectl get pods -n flink-operator
# flink-kubernetes-operator-…  2/2  Running  0  …
helm list -n flink-operator
# CHART flink-kubernetes-operator-1.15.0   APP VERSION 1.15.0   STATUS deployed
```

The migration is real when a `FlinkDeployment` on `flinkVersion: v2_2` reaches `READY`:

```bash
kubectl get flinkdeployment -n my-app \
  -o jsonpath='{.items[*].status.jobManagerDeploymentStatus}'
# READY
```

Validate on a real cluster, not only in CI. Our end-to-end gate — operator 1.15.0, `flinkVersion: v2_2`, Kafka and Kinesis I/O against a kind cluster — passed 4/4.

## What to remember

- The operator gates `flinkVersion`. Running Flink 2.2 needs operator 1.15+; the image alone isn't enough.
- The functional change is small: a Helm version bump and `flinkVersion: v2_0 → v2_2`. The CRD `apiVersion` (`v1beta1`) is unchanged.
- Audit `flinkConfiguration` conservatively. `flinkVersion` is operator-side; the runtime already parses your keys. Don't delete what you can't prove is gone.
- Pin and force-update the Helm install (`--version` and `--force-update`) so a stale repo can't install the wrong chart against your new image.
- The operator pod won't give you JSON logs without a custom image; the JM/TM will.
- Validate on a real cluster, not only in CI.

## Where to find it

- **Source:** [github.com/kzmlabs/flink-statefun][our-repo]
- **Docs:** [kzmlabs.github.io/flink-statefun][docs] — including the [Kubernetes deployment guide][k8s-guide]
- **Flink Kubernetes Operator:** [official docs][operator-docs]

[our-repo]: https://github.com/kzmlabs/flink-statefun
[docs]: https://kzmlabs.github.io/flink-statefun/
[k8s-guide]: ../guides/k8s-deployment.md
[operator-docs]: https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/
