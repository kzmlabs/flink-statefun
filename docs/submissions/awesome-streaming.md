# Submission to manuzhang/awesome-streaming

Ready-to-paste markdown for [manuzhang/awesome-streaming](https://github.com/manuzhang/awesome-streaming).

## Recommended placement

Section: **Frameworks** (alphabetical), or under a **Stateful Functions** subsection if the list adds one.

## The bullet

```markdown
- [Kzmlabs StateFun Actors](https://github.com/kzmlabs/flink-statefun) — Stateful actors on Apache Flink 2.x and Java 21. Durable per-key state, exactly-once messaging, Kafka and Kinesis I/O, Kubernetes-native deployment. Continuation of Apache Stateful Functions.
```

## How to submit

1. Fork [manuzhang/awesome-streaming](https://github.com/manuzhang/awesome-streaming)
2. Insert the bullet under **Frameworks** (after the Apache Flink entry, before Kafka Streams)
3. Open PR with title: `Add Kzmlabs StateFun Actors`
4. PR body — focus on stream-processing audience:

```markdown
Adds Kzmlabs StateFun Actors — a continuation of Apache Stateful Functions
on Apache Flink 2.x and Java 21. Re-implements the function programming
model on the modern Flink line, with exactly-once Kafka and Kinesis I/O,
Kubernetes-native deployment via the Flink Operator, and durable per-key
state. Maintained on Maven Central + GHCR with a K8s E2E gate.
```
