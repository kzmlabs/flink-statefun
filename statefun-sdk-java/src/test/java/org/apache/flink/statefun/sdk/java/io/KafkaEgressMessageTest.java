// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageWrapper;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

class KafkaEgressMessageTest {

  private static final TypeName EGRESS_ID = TypeName.typeNameOf("io.test", "kafka-egress");

  @Test
  void utf8RoundtripCarriesTopicAndUtf8Value() throws InvalidProtocolBufferException {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("orders")
            .withUtf8Value("hello")
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(message.targetEgressId()).isEqualTo(EGRESS_ID);
    assertThat(record.getTopic()).isEqualTo("orders");
    assertThat(record.getValueBytes().toStringUtf8()).isEqualTo("hello");
    assertThat(record.getKey()).isEmpty();
  }

  @Test
  void byteValueAndUtf8KeyAreCarriedThrough() throws InvalidProtocolBufferException {
    byte[] valueBytes = new byte[] {1, 2, 3, 4, 5};
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("events")
            .withUtf8Key("partition-key")
            .withValue(valueBytes)
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getKey()).isEqualTo("partition-key");
    assertThat(record.getValueBytes().toByteArray()).containsExactly(valueBytes);
  }

  @Test
  void byteKeyAndSliceTopicWork() throws InvalidProtocolBufferException {
    byte[] keyBytes = new byte[] {(byte) 'a', (byte) 'b', (byte) 'c'};
    Slice topicSlice = Slices.copyFromUtf8("dlq");
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic(topicSlice)
            .withKey(keyBytes)
            .withUtf8Value("payload")
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getTopic()).isEqualTo("dlq");
    assertThat(record.getKeyBytes().toByteArray()).containsExactly(keyBytes);
  }

  @Test
  void sliceKeyAndSliceValueAreCopied() throws InvalidProtocolBufferException {
    Slice keySlice = Slices.copyFromUtf8("k1");
    Slice valueSlice = Slices.copyOf(new byte[] {10, 20, 30});
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("topic-1")
            .withKey(keySlice)
            .withValue(valueSlice)
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getKey()).isEqualTo("k1");
    assertThat(record.getValueBytes().toByteArray()).containsExactly(10, 20, 30);
  }

  @Test
  void typedKeyAndValueGoThroughTypeSerializer() throws InvalidProtocolBufferException {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("typed")
            .withKey(Types.stringType(), "key-payload")
            .withValue(Types.integerType(), 42)
            .build();

    KafkaProducerRecord record = unpack(message);
    // String/integer types use proto-encoded slices (length-prefixed) — round-trip through the
    // serializer instead of comparing raw bytes.
    String decodedKey =
        Types.stringType()
            .typeSerializer()
            .deserialize(Slices.wrap(record.getKeyBytes().toByteArray()));
    Integer decodedValue =
        Types.integerType()
            .typeSerializer()
            .deserialize(Slices.wrap(record.getValueBytes().toByteArray()));
    assertThat(decodedKey).isEqualTo("key-payload");
    assertThat(decodedValue).isEqualTo(42);
  }

  @Test
  void byteValueOverloadIsCoveredByValueOverload() throws InvalidProtocolBufferException {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID).withTopic("t").withValue(new byte[] {7, 8, 9}).build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getValueBytes().toByteArray()).containsExactly(7, 8, 9);
  }

  @Test
  void egressMessageWrapsTypedValueWithKafkaProducerRecordTypename() {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("orders")
            .withUtf8Value("hello")
            .build();

    EgressMessageWrapper wrapper = (EgressMessageWrapper) message;
    assertThat(wrapper.typedValue().getTypename())
        .isEqualTo("type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord");
  }

  @Test
  void buildWithoutTopicThrows() {
    KafkaEgressMessage.Builder builder =
        KafkaEgressMessage.forEgress(EGRESS_ID).withUtf8Value("v");
    assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void buildWithoutValueThrows() {
    KafkaEgressMessage.Builder builder =
        KafkaEgressMessage.forEgress(EGRESS_ID).withTopic("t");
    assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void forEgressRejectsNullEgressId() {
    assertThatThrownBy(() -> KafkaEgressMessage.forEgress(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullUtf8KeyIsRejected() {
    KafkaEgressMessage.Builder builder = KafkaEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withUtf8Key(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullByteKeyIsRejected() {
    KafkaEgressMessage.Builder builder = KafkaEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withKey((byte[]) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullSliceKeyIsRejected() {
    KafkaEgressMessage.Builder builder = KafkaEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withKey((Slice) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullUtf8ValueIsRejected() {
    KafkaEgressMessage.Builder builder = KafkaEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withUtf8Value(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullByteValueIsRejected() {
    KafkaEgressMessage.Builder builder = KafkaEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withValue((byte[]) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullSliceValueIsRejected() {
    KafkaEgressMessage.Builder builder = KafkaEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withValue((Slice) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void buildIsValidWithoutKey() throws InvalidProtocolBufferException {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID).withTopic("k-less").withUtf8Value("v").build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getKey()).isEmpty();
  }

  private static KafkaProducerRecord unpack(EgressMessage message)
      throws InvalidProtocolBufferException {
    EgressMessageWrapper wrapper = (EgressMessageWrapper) message;
    return KafkaProducerRecord.parseFrom(wrapper.typedValue().getValue());
  }
}
