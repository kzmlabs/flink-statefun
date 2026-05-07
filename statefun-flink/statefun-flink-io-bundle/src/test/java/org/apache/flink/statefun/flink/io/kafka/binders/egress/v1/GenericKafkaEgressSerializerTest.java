// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.io.kafka.binders.egress.v1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import org.apache.flink.statefun.flink.common.types.TypedValueUtil;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class GenericKafkaEgressSerializerTest {

  private final GenericKafkaEgressSerializer serializer = new GenericKafkaEgressSerializer();

  @Test
  void serializesRecordWithKeyAndValueToProducerRecord() {
    TypedValue input = packAsKafkaRecord("topic-A", "key-1", "hello".getBytes(StandardCharsets.UTF_8));

    ProducerRecord<byte[], byte[]> record = serializer.serialize(input);

    assertThat(record.topic()).isEqualTo("topic-A");
    assertThat(record.key()).isEqualTo("key-1".getBytes(StandardCharsets.UTF_8));
    assertThat(record.value()).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void serializesRecordWithoutKeyOmitsKeyByte() {
    // An empty key should produce a ProducerRecord with null key — partitioner falls back to
    // round-robin / sticky semantics rather than treating "" as a real key.
    TypedValue input = packAsKafkaRecord("topic-A", "", "v".getBytes(StandardCharsets.UTF_8));

    ProducerRecord<byte[], byte[]> record = serializer.serialize(input);

    assertThat(record.key()).isNull();
    assertThat(record.value()).isEqualTo("v".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void rejectsTypedValueOfWrongProtobufType() {
    // TypedValue carrying a message that is NOT a KafkaProducerRecord.
    TypedValue notAKafkaRecord =
        TypedValueUtil.packProtobufMessage(TypedValue.getDefaultInstance());

    assertThatThrownBy(() -> serializer.serialize(notAKafkaRecord))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("KafkaProducerRecord");
  }

  @Test
  void rejectsCorruptedProtobufBytes() {
    // Build a TypedValue that *claims* to be a KafkaProducerRecord but carries garbage bytes.
    TypedValue corrupted =
        TypedValue.newBuilder()
            .setTypename("type.googleapis.com/" + KafkaProducerRecord.getDescriptor().getFullName())
            .setHasValue(true)
            .setValue(ByteString.copyFrom(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}))
            .build();

    assertThatThrownBy(() -> serializer.serialize(corrupted))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Unable to unpack");
  }

  @Test
  void serializerIsSerializableForFlinkOperatorChain() throws Exception {
    // Flink requires KafkaEgressSerializer impls to be Java-serializable so they can be sent
    // along with the JobGraph to TaskManagers.
    java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
    new java.io.ObjectOutputStream(bytes).writeObject(serializer);

    Object copy =
        new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray()))
            .readObject();

    assertThat(copy).isInstanceOf(GenericKafkaEgressSerializer.class);
  }

  private static TypedValue packAsKafkaRecord(String topic, String key, byte[] value) {
    KafkaProducerRecord rec =
        KafkaProducerRecord.newBuilder()
            .setTopic(topic)
            .setKey(key)
            .setValueBytes(ByteString.copyFrom(value))
            .build();
    return TypedValueUtil.packProtobufMessage(rec);
  }
}
