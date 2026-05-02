# End-to-end tests

Kzmlabs StateFun runs a full Kubernetes-native end-to-end test as a mandatory gate before any release. The test exercises the same topology a production deployment would use — Flink Kubernetes Operator, real Kafka, real S3-compatible storage, RocksDB checkpointing, the actual remote-function HTTP server.

## Stack

- **`kind`** — local Kubernetes cluster, single node, provisioned per test run
- **Flink Kubernetes Operator 1.11.0** — the same Operator a production user would deploy
- **Apache Kafka 3.9** — single-broker KRaft mode, dual listener (cluster + port-forward)
- **LocalStack 4.1** — emulates Kinesis (for the Kinesis test path) and S3 (for checkpoint storage)
- **Remote function HTTP server** — multi-stage jlink-stripped Alpine image (~80 MB)
- **`FlinkDeployment` CR** — RocksDB state backend, S3 checkpoints, leader-election tuned for fast startup

## Coverage

Two test classes run in the same `mvn verify` invocation:

| Test | Validates |
|---|---|
| `StateFunK8sE2E` | Kafka ingress → stateful counter function → Kafka egress; greeter function over JSON; checkpoint persistence |
| `StateFunKinesisE2E` | Kinesis ingress → stateful counter → Kinesis egress; ARN-keyed routing; LocalStack-backed S3 checkpoints |

JUnit 5 `@Tag("kafka")` / `@Tag("kinesis")` allow running either suite in isolation:

```bash
./mvnw verify -pl :statefun-e2e-k8s-native -Dgroups=kinesis
```

## CI integration

The reusable workflow `.github/workflows/e2e-test.yml` is invoked:

- On every PR (mandatory gate)
- On push to `release` (post-merge validation)
- By the release pipeline (gates Maven Central publish + GHCR image push)

Concurrency is configured so that repeated triggers on the same ref auto-cancel earlier runs — an important guard given the run cost.

## Local execution

```bash
./mvnw verify -pl :statefun-e2e-k8s-native -am
```

Provisions a kind cluster (creating it if absent), runs the suite, tears down. Set `-Dskip.teardown=true` to keep the cluster after the test for debugging:

```bash
./mvnw verify -pl :statefun-e2e-k8s-native -am -Dskip.teardown=true
kubectl get pods -n statefun-e2e
kubectl logs -n statefun-e2e -l component=jobmanager
```

## Restricted-network override

Set `IMAGE_REGISTRY_PREFIX` to wire base images through an internal mirror:

```bash
export IMAGE_REGISTRY_PREFIX=harbor.example.com/dockerhub-proxy/
./mvnw verify -pl :statefun-e2e-k8s-native -am
```

Apache Kafka, LocalStack, and the JDK base images all honour the prefix — no fork of the manifests required.
