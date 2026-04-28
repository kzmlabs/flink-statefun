// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;

final class KafkaSerializationSchemaDelegate<T> implements KafkaRecordSerializationSchema<T> {

  private static final long serialVersionUID = 1L;

  private final KafkaEgressSerializer<T> serializer;

  KafkaSerializationSchemaDelegate(KafkaEgressSerializer<T> serializer) {
    this.serializer = Objects.requireNonNull(serializer);
  }

  @Nullable
  @Override
  public ProducerRecord<byte[], byte[]> serialize(
      T t, KafkaSinkContext kafkaSinkContext, Long aLong) {
    return serializer.serialize(t);
  }
}
