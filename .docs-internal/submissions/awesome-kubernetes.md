# Submission to ramitsurana/awesome-kubernetes

Ready-to-paste markdown for [ramitsurana/awesome-kubernetes](https://github.com/ramitsurana/awesome-kubernetes).

## Recommended placement

Section: **Stream Processing** or **Data Processing** if the list has one. Otherwise under **Frameworks** alongside other workload frameworks (e.g., Flink Operator, Spark Operator entries).

## The bullet — framed for Kubernetes audience

```markdown
- [Kzmlabs StateFun Actors](https://github.com/kzmlabs/flink-statefun) — Stateful actor framework deployable on Kubernetes via the Apache Flink Operator. Durable per-key state, exactly-once messaging, Kafka and Kinesis I/O. Tested on Java 21 with a kind-based E2E gate in CI.
```

## How to submit

1. Fork [ramitsurana/awesome-kubernetes](https://github.com/ramitsurana/awesome-kubernetes)
2. Find the appropriate section (search for "Flink Operator" — neighboring placement)
3. Open PR with title: `Add Kzmlabs StateFun Actors`
4. PR body — focus on K8s deployment story:

```markdown
Adds Kzmlabs StateFun Actors — a stateful actor framework that ships as a
container image (ghcr.io/kzmlabs/flink-statefun) and deploys on Kubernetes
via the Apache Flink Operator. The repo includes a kind-based end-to-end
test that provisions Flink Operator + Kafka + LocalStack and runs both
Kafka and Kinesis ingress paths against a real Flink cluster on every
release — so the K8s deployment story is exercised in CI rather than
just documented.
```

## Pitch angle for Kubernetes readers

The K8s/Flink Operator pattern is well-known but the audience often hits the same question: "how do I keep durable state across pod restarts and rescaling?" StateFun Actors gives them an opinionated answer — keyed state in Flink's checkpoint backend, S3 (or any S3-compatible store) holds the snapshots, the Operator handles savepoint-on-rolling-update. That story doesn't get told often enough.
