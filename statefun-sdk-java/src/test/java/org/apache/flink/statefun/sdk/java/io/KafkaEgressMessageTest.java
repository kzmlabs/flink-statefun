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
  void headersAreCarriedThroughInOrderIncludingDuplicateKeys()
      throws InvalidProtocolBufferException {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("orders")
            .withUtf8Value("hello")
            .withUtf8Header("trace-id", "abc-123")
            .withHeader("payload-hash", new byte[] {1, 2, 3})
            .withHeader("trace-id", Slices.copyFromUtf8("def-456"))
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getHeadersCount()).isEqualTo(3);
    assertThat(record.getHeaders(0).getKey()).isEqualTo("trace-id");
    assertThat(record.getHeaders(0).getValue().toStringUtf8()).isEqualTo("abc-123");
    assertThat(record.getHeaders(1).getKey()).isEqualTo("payload-hash");
    assertThat(record.getHeaders(1).getValue().toByteArray()).containsExactly(1, 2, 3);
    assertThat(record.getHeaders(2).getKey()).isEqualTo("trace-id");
    assertThat(record.getHeaders(2).getValue().toStringUtf8()).isEqualTo("def-456");
  }

  @Test
  void headerValuesSupportBytesStringAndTypedPrimitives() throws InvalidProtocolBufferException {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("typed-headers")
            .withUtf8Value("v")
            .withHeader("raw-bytes", new byte[] {1, 2, 3})
            .withUtf8Header("str", "hello")
            .withHeader("int", Types.integerType(), 42)
            .withHeader("long", Types.longType(), 42_000_000_000L)
            .withHeader("double", Types.doubleType(), 3.14d)
            .withHeader("bool", Types.booleanType(), true)
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getHeadersCount()).isEqualTo(6);
    assertThat(record.getHeaders(0).getValue().toByteArray()).containsExactly(1, 2, 3);
    assertThat(record.getHeaders(1).getValue().toStringUtf8()).isEqualTo("hello");
    assertThat(decode(record, 2, Types.integerType())).isEqualTo(42);
    assertThat(decode(record, 3, Types.longType())).isEqualTo(42_000_000_000L);
    assertThat(decode(record, 4, Types.doubleType())).isEqualTo(3.14d);
    assertThat(decode(record, 5, Types.booleanType())).isTrue();
  }

  @Test
  void recordWithoutHeadersHasEmptyHeaderList() throws InvalidProtocolBufferException {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID).withTopic("t").withUtf8Value("v").build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getHeadersCount()).isZero();
  }


  @Test
  void nullHeaderValuesArePreservedAsNullNotEmpty() throws InvalidProtocolBufferException {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("t")
            .withUtf8Value("v")
            .withUtf8Header("a", null)
            .withHeader("b", (byte[]) null)
            .withHeader("c", (Slice) null)
            .withHeader("d", Types.stringType(), null)
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getHeadersList())
        .hasSize(4)
        .allMatch(header -> !header.getHasValue() && header.getValue().isEmpty());
  }

  @Test
  void presentHeaderValuesAreMarkedHasValueIncludingExplicitlyEmptyOnes()
      throws InvalidProtocolBufferException {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("t")
            .withUtf8Value("v")
            .withHeader("explicit-empty", new byte[0])
            .withUtf8Header("present", "x")
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getHeadersList()).hasSize(2).allMatch(KafkaProducerRecord.Header::getHasValue);
    assertThat(record.getHeaders(0).getValue().isEmpty()).isTrue();
  }

  @Test
  void nullHeaderKeyDegradesToEmptyKeyInsteadOfFailingTheSend()
      throws InvalidProtocolBufferException {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("t")
            .withUtf8Value("v")
            .withUtf8Header(null, "orphan")
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getHeaders(0).getKey()).isEmpty();
    assertThat(record.getHeaders(0).getValue().toStringUtf8()).isEqualTo("orphan");
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

  private static <T> T decode(
      KafkaProducerRecord record, int headerIndex, org.apache.flink.statefun.sdk.java.types.Type<T> type) {
    return type.typeSerializer()
        .deserialize(Slices.wrap(record.getHeaders(headerIndex).getValue().toByteArray()));
  }
}
