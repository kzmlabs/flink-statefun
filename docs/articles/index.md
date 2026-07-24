---
title: Articles
description: Long-form notes on the StateFun Actors continuation — design decisions, migration playbooks, and lessons from running stateful actors on Apache Flink 2.x.
---

# Articles

Long-form notes from the maintainers — the design decisions, migration playbooks, and engineering lessons behind running stateful actors on Apache Flink 2.x.

## Latest

- [**Kafka record headers in Stateful Functions — closing a five-year-old gap**](kafka-record-headers.md)
  End-to-end header support lands in KZM-3.4: trace propagation through function graphs, typed header values, Kafka-exact null semantics, per-topic opt-in — and the protocol trick that made it possible without touching the runtime's hot path. *2026-07-24*

- [**We forked Apache Stateful Functions for Flink 2.x — here's why**](forking-statefun.md)
  Why the project exists, what we changed under the hood, what stayed the same for upstream users, and where this fits versus Kafka Streams, ksqlDB, and Akka. *2026-05-03*
