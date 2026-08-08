---
title: Metrics
description: Metrics StateFun Actors adds on top of Flink's own - the invalid-record counters of the routable Kafka ingress, their registration scope, naming conventions, and Prometheus mapping.
---

# Metrics

StateFun Actors jobs expose the standard [Flink metrics](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/) unchanged. This page documents what this distribution adds on top. The reference deployments enable the Prometheus reporter on port 9249 (`metrics.reporter.prom.factory.class`, see `flink-deployment.yaml` in the E2E resources).

## Invalid-record counters

Registered on the source operator's metric group of the routable Kafka ingress. They increment once per record skipped under `invalidRecordHandling: skip` (see [Kafka I/O](kafka-io.md#invalid-records)); under `type: fail` nothing increments because the first invalid record fails the job.

Both counters register inside the `deserializer` subgroup that KafkaSource hands to the deserialization schema, so their metric name lives under `<operator>.deserializer.*`:

| Counter | Increments | Dimensions |
|---|---|---|
| `numInvalidRecordsSkipped` | once per skipped record, all topics of the ingress | none |
| `numInvalidRecordsSkipped` | once per skipped record of that topic and defect | `topic`, `defect` (key-value metric groups) |

The per-topic breakdown uses `addGroup("topic", …).addGroup("defect", …)` - the two-argument, key-value form. `topic` and `defect` are the group **keys**, so they become dimensions (labels/tags in reporters that support them), not part of the metric name. `<defect>` is `NULL_KEY` or `NULL_VALUE`, matching the `defect [...]` field of the per-record skip log line.

The [FLIP-33](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics) standard `numRecordsInErrors` counter is deliberately not touched: it lives on the operator I/O metric group, which is not reachable from the deserializer context, and a same-named counter in a different scope would only mislead dashboards.

## How each reporter renders it

The two representations are the same metric, rendered differently per reporter - this trips people up, so both are spelled out:

- **JMX / Slf4j / other label-less reporters** fold the key-value groups into the logical identifier: `…deserializer.topic.<topic>.defect.<defect>.numInvalidRecordsSkipped`. Here `topic`/`defect` and their values *are* part of the name.
- **Prometheus** turns key-value groups into labels. The metric name is `flink_taskmanager_job_task_operator_deserializer_numInvalidRecordsSkipped` and the breakdown arrives as labels: `{topic="example.orders", defect="NULL_VALUE"}`. The values are **not** in the name - query by label. Alert rules: [Alerting](alerting.md).
