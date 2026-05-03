# Submission to infoslack/awesome-kafka

Ready-to-paste markdown for [infoslack/awesome-kafka](https://github.com/infoslack/awesome-kafka).

## Recommended placement

Section: **Frameworks** or **Stream Processing** — wherever Kafka Streams and Faust live.

## The bullet — framed for Kafka audience

```markdown
- [Kzmlabs StateFun Actors](https://github.com/kzmlabs/flink-statefun) — Stateful actor framework on Apache Flink 2.x with first-class Kafka I/O. Exactly-once delivery, durable per-key state, JSON / Avro / Protobuf payloads, deployable on Kubernetes via the Flink Operator.
```

## How to submit

1. Fork [infoslack/awesome-kafka](https://github.com/infoslack/awesome-kafka)
2. Insert the bullet alphabetically under the appropriate section
3. Open PR with title: `Add Kzmlabs StateFun Actors`
4. PR body — focus on Kafka integration:

```markdown
Adds Kzmlabs StateFun Actors — a stateful actor framework on Apache Flink 2.x
with native Kafka ingress and egress. Provides exactly-once delivery,
per-key durable state, and a programming model where each Kafka key
becomes an addressable function with its own persisted state. Useful for
fraud detection, real-time aggregation, and CDC consumers that need
state larger than what Kafka Streams' rocksdb store handles comfortably.
```

## Pitch angle for Kafka readers

Kafka Streams is great when state fits the same-process rocksdb store, but it tops out around hundreds of GB per task and doesn't easily share state across topologies. StateFun Actors moves the state into Flink's keyed state backend (RocksDB or HashMap state with tiered checkpoints to S3) — this is where the audience overlap with Kafka users gets interesting.
