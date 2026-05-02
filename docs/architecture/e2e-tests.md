---
title: End-to-end tests
description: The Kubernetes-native end-to-end test gate that runs before every StateFun Actors release on Apache Flink — kind cluster, Flink Operator, Kafka, Kinesis-via-LocalStack, RocksDB checkpoints to S3.
---

# End-to-end tests

> Every release is gated on a real Kubernetes cluster running the real Flink Operator with the real Kafka and the real remote-function HTTP server. Not unit tests pretending to be integration tests — the actual production topology.

## Stack

| Component | What it provides |
|---|---|
| **`kind`** | A throwaway single-node Kubernetes cluster, provisioned per test run |
| **Flink Kubernetes Operator 1.11** | Same Operator a production user would deploy |
| **Apache Kafka 3.9** | Single-broker KRaft mode, dual listener (cluster + port-forward) |
| **LocalStack 4.1** | Emulates Kinesis (Kinesis test path) and S3 (checkpoint storage) |
| **Remote function pod** | Multi-stage jlink-stripped Alpine image, ~80 MB |
| **`FlinkDeployment` CR** | RocksDB state backend, S3 checkpoints, leader-election tuned for fast startup |

## Coverage

Two test classes run in the same `mvn verify` invocation:

| Test | Validates |
|---|---|
| `StateFunK8sE2E` | Kafka ingress → stateful counter function → Kafka egress; greeter function over JSON; checkpoint persistence |
| `StateFunKinesisE2E` | Kinesis ingress → stateful counter → Kinesis egress; ARN-keyed routing; LocalStack-backed S3 checkpoints |

JUnit 5 `@Tag("kafka")` / `@Tag("kinesis")` allow running either suite in isolation:

```bash
./mvnw verify -pl :statefun-e2e-k8s-native -am -Dgroups=kinesis        # Kinesis only
./mvnw verify -pl :statefun-e2e-k8s-native -am -DexcludedGroups=kinesis  # Kafka only
```

## CI integration

The reusable workflow `.github/workflows/e2e-test.yml` is invoked:

- **On every PR** — mandatory gate. No PR merges without a green E2E.
- **On push to `release`** — post-merge validation.
- **By the release pipeline** — gates Maven Central publish and GHCR image push.

Concurrency is configured so that repeated triggers on the same ref auto-cancel earlier in-progress runs — important guard given the run cost (~25 min on a GH-hosted runner).

## Local execution

```bash
./mvnw verify -pl :statefun-e2e-k8s-native -am
```

Provisions a kind cluster (creating it if absent), runs the suite, tears down.

!!! tip "Keep the cluster after the test for debugging"

    ```bash
    ./mvnw verify -pl :statefun-e2e-k8s-native -am -Dskip.teardown=true
    kubectl get pods -n statefun-e2e
    kubectl logs -n statefun-e2e -l component=jobmanager --tail=200
    ```

    Tear down manually when done: `kind delete cluster --name statefun-e2e`.

## Restricted-network override

Set `IMAGE_REGISTRY_PREFIX` to wire base images through an internal mirror:

```bash
export IMAGE_REGISTRY_PREFIX=harbor.example.com/dockerhub-proxy/
./mvnw verify -pl :statefun-e2e-k8s-native -am
```

Apache Kafka, LocalStack, and the JDK base images all honour the prefix — no manifest fork required.

## What "real" means

The E2E gate uses the same Operator, the same image, the same Kafka, and the same wire protocol that a production deployment would. The only difference is scale — single broker, single replica, kind instead of EKS/GKE/AKS.

Specifically:

- **The Flink Operator** is unmodified upstream 1.11
- **Kafka** runs the apache/kafka image (not a mock)
- **The remote function** is built from the same Dockerfile as production fixtures, just under a different artifact name
- **Checkpoints** are written to LocalStack S3 with the same Flink S3 plugin used in production
- **The JM/TM topology** matches what a `FlinkDeployment` produces in cloud

A test that passes in this environment has been exercised against the actual production paths — there's no mock layer to silently diverge.

## Next steps

<div class="grid cards" markdown>

- :material-graph:{ .lg .middle } &nbsp; **[Architecture overview](index.md)** — system context for the E2E topology.
- :material-kubernetes:{ .lg .middle } &nbsp; **[Production K8s deployment](../guides/k8s-deployment.md)** — same shape, real cloud.
- :material-source-branch-sync:{ .lg .middle } &nbsp; **[Release process](../release-process.md)** — how the E2E gate fits into release CI.

</div>
