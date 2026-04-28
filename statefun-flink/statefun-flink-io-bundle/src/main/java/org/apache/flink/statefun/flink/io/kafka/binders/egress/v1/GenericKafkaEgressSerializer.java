// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka.binders.egress.v1;

import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.statefun.flink.common.types.TypedValueUtil;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * A {@link KafkaEgressSerializer} used solely by Kafka egresses bound by {@link
 * GenericKafkaEgressBinderV1}.
 *
 * <p>This serializer expects Protobuf messages of type {@link KafkaProducerRecord}, and simply
 * transforms those into Kafka's {@link ProducerRecord}.
 */
public final class GenericKafkaEgressSerializer implements KafkaEgressSerializer<TypedValue> {

  private static final long serialVersionUID = 1L;

  @Override
  public ProducerRecord<byte[], byte[]> serialize(TypedValue message) {
    KafkaProducerRecord protobufProducerRecord = asKafkaProducerRecord(message);
    return toProducerRecord(protobufProducerRecord);
  }

  private static KafkaProducerRecord asKafkaProducerRecord(TypedValue message) {
    if (!TypedValueUtil.isProtobufTypeOf(message, KafkaProducerRecord.getDescriptor())) {
      throw new IllegalStateException(
          "The generic Kafka egress expects only messages of type "
              + KafkaProducerRecord.class.getName());
    }
    try {
      return KafkaProducerRecord.parseFrom(message.getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          "Unable to unpack message as a " + KafkaProducerRecord.class.getName(), e);
    }
  }

  private static ProducerRecord<byte[], byte[]> toProducerRecord(
      KafkaProducerRecord protobufProducerRecord) {
    final String key = protobufProducerRecord.getKey();
    final String topic = protobufProducerRecord.getTopic();
    final byte[] valueBytes = protobufProducerRecord.getValueBytes().toByteArray();

    if (key == null || key.isEmpty()) {
      return new ProducerRecord<>(topic, valueBytes);
    } else {
      return new ProducerRecord<>(topic, key.getBytes(StandardCharsets.UTF_8), valueBytes);
    }
  }
}
