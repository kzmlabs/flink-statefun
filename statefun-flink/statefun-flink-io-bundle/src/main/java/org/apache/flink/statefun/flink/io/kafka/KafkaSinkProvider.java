// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.statefun.flink.io.common.ReflectionUtil;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaProducerSemantic;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaSinkProvider implements SinkProvider {

  @Override
  public <T> Sink<T> forSpec(EgressSpec<T> egressSpec) {
    KafkaEgressSpec<T> spec = asSpec(egressSpec);
    DeliveryGuarantee producerSemantic = semanticFromSpec(spec);

    KafkaSinkBuilder<T> sinkBuilder =
        KafkaSink.<T>builder()
            .setBootstrapServers(spec.kafkaAddress())
            .setRecordSerializer(serializationSchemaFromSpec(spec))
            .setDeliveryGuarantee(producerSemantic);

    spec.properties().forEach((k, v) -> sinkBuilder.setProperty(k.toString(), v.toString()));

    if (producerSemantic == DeliveryGuarantee.EXACTLY_ONCE) {
      long transactionTimeout =
          spec.semantic().asExactlyOnceSemantic().transactionTimeout().toMillis();
      sinkBuilder.setProperty(
          ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(transactionTimeout));
    }

    return sinkBuilder.build();
  }

  private <T> KafkaRecordSerializationSchema<T> serializationSchemaFromSpec(
      KafkaEgressSpec<T> spec) {
    KafkaEgressSerializer<T> serializer = ReflectionUtil.instantiate(spec.serializerClass());
    return new KafkaSerializationSchemaDelegate<>(serializer);
  }

  private static <T> DeliveryGuarantee semanticFromSpec(KafkaEgressSpec<T> spec) {
    final KafkaProducerSemantic semantic = spec.semantic();
    if (semantic.isExactlyOnceSemantic()) {
      return DeliveryGuarantee.EXACTLY_ONCE;
    } else if (semantic.isAtLeastOnceSemantic()) {
      return DeliveryGuarantee.AT_LEAST_ONCE;
    } else if (semantic.isNoSemantic()) {
      return DeliveryGuarantee.NONE;
    } else {
      throw new IllegalArgumentException("Unknown producer semantic " + semantic.getClass());
    }
  }

  private static <T> KafkaEgressSpec<T> asSpec(EgressSpec<T> spec) {
    if (spec instanceof KafkaEgressSpec) {
      return (KafkaEgressSpec<T>) spec;
    }
    if (spec == null) {
      throw new NullPointerException("Unable to translate a NULL spec");
    }
    throw new IllegalArgumentException(String.format("Wrong type %s", spec.type()));
  }
}
