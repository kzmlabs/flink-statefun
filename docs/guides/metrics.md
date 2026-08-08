---
title: Metrics
description: Metrics StateFun Actors adds on top of Flink's own - the invalid-record counters of the routable Kafka ingress, their registration scope, naming conventions, and Prometheus mapping.
---

# Metrics

StateFun Actors jobs expose the standard [Flink metrics](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/) unchanged. This page documents what this distribution adds on top. The reference deployments enable the Prometheus reporter on port 9249 (`metrics.reporter.prom.factory.class`, see `flink-deployment.yaml` in the E2E resources).

## Invalid-record counters

Registered on the source operator's metric group of the routable Kafka ingress. They increment once per record skipped under `invalidRecordHandling: skip` (see [Kafka I/O](kafka-io.md#invalid-records)); under `type: fail` nothing increments because the first invalid record fails the job.

Both counters register inside the `deserializer` subgroup that KafkaSource hands to the deserialization schema, so their full scope is `<operator>.deserializer.*`:

| Metric | Increments | Naming rationale |
|---|---|---|
| `deserializer.numInvalidRecordsSkipped` | once per skipped record, all topics of the ingress | Flink operator-counter convention `numXxx`, cf. `numLateRecordsDropped` |
| `deserializer.topic.<topic>.defect.<defect>.numInvalidRecordsSkipped` | once per skipped record of that topic and defect | key-value metric groups, same pattern as the Kafka connector's `KafkaSourceReader.topic.<topic>.partition.<partition>.*` |

`<defect>` is `NULL_KEY` or `NULL_VALUE`, matching the `defect [...]` field of the per-record skip log line.

The [FLIP-33](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics) standard `numRecordsInErrors` counter is deliberately not touched: it lives on the operator I/O metric group, which is not reachable from the deserializer context, and a same-named counter in a different scope would only mislead dashboards.

## Prometheus mapping

Key-value groups become labels: the per-topic counter arrives as `..._deserializer_topic_defect_numInvalidRecordsSkipped{topic="example.orders", defect="NULL_VALUE"}`. The metric name prefix depends on the reporter's scope format; the trailing `deserializer_topic_defect_numInvalidRecordsSkipped` part is stable. Alert rules for these counters: [Alerting](alerting.md).
