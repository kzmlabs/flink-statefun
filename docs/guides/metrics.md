---
title: Metrics
description: Metrics exposed by StateFun Actors on top of Flink's own - invalid-record counters on the Kafka ingress, their scopes, and how to alert on them.
---

# Metrics

StateFun Actors jobs expose all standard [Flink metrics](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/); the E2E and quickstart deployments ship with the Prometheus reporter enabled on port 9249. This page documents the metrics this distribution adds on top.

## Invalid records on the Kafka ingress

Emitted by the routable Kafka ingress when a record is skipped under `invalidRecordHandling: skip` (see the [Kafka I/O guide](kafka-io.md#invalid-records)). All three are counters registered on the source operator's metric group and increment once per skipped record:

| Metric | Scope | Notes |
|---|---|---|
| `numInvalidRecordsSkipped` | source operator | Total skipped records across all topics of the ingress. Follows Flink's `numXxx` counter convention (cf. `numLateRecordsDropped`). |
| `numRecordsInErrors` | source operator | The FLIP-33 standard source counter - existing dashboards that already chart it pick up the signal unchanged. |
| `topic.<name>.numInvalidRecordsSkipped` | source operator, per topic | Same count broken down per topic via a `topic` metric group (the Kafka connector's group convention), so one bad producer is attributable and alertable in isolation. |

Under `type: fail` no counters increment - the job fails on the first invalid record, which is its own signal.

## Alerting example

With the Prometheus reporter, the per-topic group becomes a `topic` label. Alert on any skipped record per topic over 5 minutes:

```yaml
- alert: StateFunInvalidRecordsSkipped
  expr: increase(flink_taskmanager_job_task_operator_topic_numInvalidRecordsSkipped[5m]) > 0
  labels:
    severity: warning
  annotations:
    summary: "Invalid Kafka records skipped on topic {{ $labels.topic }}"
    description: "A producer is emitting records the ingress cannot route (null key or tombstone). Every record is individually logged on the TaskManager - grep 'Skipping invalid record'."
```

The exact metric name prefix depends on your reporter's scope format; the trailing `topic_numInvalidRecordsSkipped` part is stable.

## Next steps

- [Kafka I/O guide](kafka-io.md#invalid-records) - the `invalidRecordHandling` policy the counters belong to.
- [Logging](logging.md) - the per-record skip log lines these counters summarize.
