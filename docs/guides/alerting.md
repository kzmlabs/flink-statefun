---
title: Alerting
description: Prometheus alert rules for the invalid-record counters of the routable Kafka ingress - per-topic, per-defect attribution and migration from restart-based alerts.
---

# Alerting

Alert rules for the counters documented in [Metrics](metrics.md). Prerequisite: a Prometheus reporter scraping the TaskManagers (port 9249 in the reference deployments). The metric name prefix below follows the default scope format; adjust it to your `metrics.scope.*` configuration - the `deserializer_topic_defect_numInvalidRecordsSkipped` suffix is stable.

## Invalid records

The per-topic, per-defect counter carries `topic` and `defect` as labels, so a firing alert names the offending topic and the kind of corruption without log correlation:

Both expressions aggregate with `sum by (topic, defect)`: a parallel ingress emits one series per source subtask, and without aggregation each subtask would fire its own alert.

```yaml
- alert: StateFunInvalidRecordsSkipped
  expr: sum by (topic, defect) (increase(flink_taskmanager_job_task_operator_deserializer_topic_defect_numInvalidRecordsSkipped[5m])) > 0
  labels:
    severity: warning
  annotations:
    summary: "Invalid Kafka records skipped on topic {{ $labels.topic }} ({{ $labels.defect }})"
    description: "Records with defect {{ $labels.defect }} are being skipped by the routable ingress. Each record is logged individually on the TaskManager: grep 'Skipping invalid record'."
```

To page on a sustained stream rather than a single stray record, alert on the rate instead:

```yaml
- alert: StateFunInvalidRecordsSustained
  expr: sum by (topic, defect) (rate(flink_taskmanager_job_task_operator_deserializer_topic_defect_numInvalidRecordsSkipped[15m])) > 0.1
  labels:
    severity: critical
  annotations:
    summary: "Sustained invalid-record stream on topic {{ $labels.topic }} ({{ $labels.defect }})"
```

## Migrating from restart-based alerts

Before `invalidRecordHandling` existed, one invalid record crashed the job, so restart alerts doubled as bad-data alerts. With the `skip` default, bad data surfaces through the counters above and restarts indicate genuine failures only. Ingresses pinned to `type: fail` keep the old signal.
