---
title: Decision records
description: Architecture Decision Records for StateFun Actors, one short document per significant technical decision, with context, the decision itself and its consequences.
---

# Decision records

Every significant technical decision in this fork is captured as an Architecture Decision Record (ADR): a short, numbered document describing the context, the decision and its consequences. ADRs are immutable once accepted; a later decision that changes course gets a new ADR that supersedes the old one.

Together with the [architecture overview](../architecture/index.md) and the [differences from Apache StateFun](../upstream-vs-kzm.md) matrix, the ADR log is the map of how this project got to its current shape and why.

## Log

| # | Decision | Status | Date |
|---|----------|--------|------|
| [0001](0001-fork-statefun-for-flink-2.md) | Fork Stateful Functions for Flink 2.x | Accepted | 2026-04 |
| [0002](0002-kafka-io-source-sink-v2.md) | Kafka I/O on Source V2 / Sink V2 | Accepted | 2026-04 |
| [0003](0003-restore-kinesis-io-localstack.md) | Restore Kinesis I/O, LocalStack-based E2E | Accepted | 2026-04 |
| [0004](0004-k8s-native-e2e-release-gate.md) | Kubernetes-native E2E as the release gate | Accepted | 2026-04 |
| [0005](0005-kafka-record-headers.md) | Kafka record headers via TypedValue metadata | Accepted | 2026-07 |
| [0006](0006-operator-1-15-flink-v2-2.md) | Flink Operator 1.15, flinkVersion v2_2 | Accepted | 2026-06 |
| [0007](0007-restart-strategy-key-names.md) | Restart-strategy keys without the execution. prefix | Accepted | 2026-07 |
| [0008](0008-kafka-invalid-record-handling.md) | Kafka invalid-record handling policy | Proposed | 2026-07 |

## Writing a new ADR

Copy the structure of any accepted ADR (frontmatter, status table, Context / Decision / Consequences). Number it with the next free index, add it to the table above and to the site navigation, and link it from the relevant row of the [differences matrix](../upstream-vs-kzm.md). Feature pull requests that change architecture or user-facing behavior should include their ADR in the same PR.
