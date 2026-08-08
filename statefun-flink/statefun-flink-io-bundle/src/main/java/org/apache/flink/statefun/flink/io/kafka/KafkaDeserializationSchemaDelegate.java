// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.statefun.flink.common.UnimplementedTypeInfo;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

final class KafkaDeserializationSchemaDelegate<T> implements KafkaRecordDeserializationSchema<T> {

  private static final long serialVersionUID = 1;

  private final TypeInformation<T> producedTypeInfo;
  private final KafkaIngressDeserializer<T> delegate;

  private transient Counter numInvalidRecordsSkipped;
  private transient Counter numRecordsInErrors;

  KafkaDeserializationSchemaDelegate(KafkaIngressDeserializer<T> delegate) {
    this.producedTypeInfo = new UnimplementedTypeInfo<>();
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public void open(DeserializationSchema.InitializationContext context) throws Exception {
    MetricGroup metricGroup = context.getMetricGroup();
    numInvalidRecordsSkipped = metricGroup.counter("numInvalidRecordsSkipped");
    numRecordsInErrors = metricGroup.counter("numRecordsInErrors");
    if (delegate instanceof InvalidRecordMetricsAware metricsAware) {
      metricsAware.registerInvalidRecordMetrics(metricGroup);
    }
  }

  @Override
  public TypeInformation<T> getProducedType() {
    // this would never be actually used, it would be replaced during translation with the type
    // information
    // of IngressIdentifier's producedType.
    // see: Sources#setOutputType.
    // if this invriant would not hold in the future, this type information would produce a
    // serialier
    // that fails immediately.
    return producedTypeInfo;
  }

  /**
   * A null from the deserializer means the record was invalid and its policy said skip: it is
   * counted (globally plus FLIP-33 numRecordsInErrors; the labeled per-topic per-defect breakdown
   * is registered by the deserializer itself via InvalidRecordMetricsAware) and not collected. The
   * valid-record path pays only one null comparison on top of the pre-existing behavior.
   */
  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<T> collector) {
    T value = delegate.deserialize(consumerRecord);
    if (value == null) {
      if (numInvalidRecordsSkipped != null) {
        numInvalidRecordsSkipped.inc();
        numRecordsInErrors.inc();
      }
      return;
    }
    collector.collect(value);
  }
}
