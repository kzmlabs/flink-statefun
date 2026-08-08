// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka;

import org.apache.flink.metrics.MetricGroup;

/**
 * Optional capability of a KafkaIngressDeserializer: when implemented, the deserialization schema
 * delegate hands over the source operator's metric group in open(), letting the deserializer
 * register richer metrics than the delegate's global counters - the routable ingress uses it for
 * the per-topic, per-defect numInvalidRecordsSkipped breakdown.
 */
public interface InvalidRecordMetricsAware {

  void registerInvalidRecordMetrics(MetricGroup metricGroup);
}
