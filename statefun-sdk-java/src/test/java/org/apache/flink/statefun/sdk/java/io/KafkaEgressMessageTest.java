// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.io;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageWrapper;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

public class KafkaEgressMessageTest {

  private static final TypeName EGRESS_ID = TypeName.typeNameOf("io.test", "kafka-egress");

  @Test
  public void utf8RoundtripCarriesTopicAndUtf8Value() throws InvalidProtocolBufferException {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("orders")
            .withUtf8Value("hello")
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(message.targetEgressId(), is(EGRESS_ID));
    assertThat(record.getTopic(), is("orders"));
    assertThat(record.getValueBytes().toStringUtf8(), is("hello"));
    assertThat(record.getKey(), is(""));
  }

  @Test
  public void byteValueAndUtf8KeyAreCarriedThrough() throws InvalidProtocolBufferException {
    byte[] valueBytes = new byte[] {1, 2, 3, 4, 5};
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("events")
            .withUtf8Key("partition-key")
            .withValue(valueBytes)
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getKey(), is("partition-key"));
    assertThat(record.getValueBytes().toByteArray(), is(valueBytes));
  }

  @Test
  public void byteKeyAndSliceTopicWork() throws InvalidProtocolBufferException {
    byte[] keyBytes = new byte[] {(byte) 'a', (byte) 'b', (byte) 'c'};
    Slice topicSlice = Slices.copyFromUtf8("dlq");
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic(topicSlice)
            .withKey(keyBytes)
            .withUtf8Value("payload")
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getTopic(), is("dlq"));
    assertThat(record.getKeyBytes().toByteArray(), is(keyBytes));
  }

  @Test
  public void sliceKeyAndSliceValueAreCopied() throws InvalidProtocolBufferException {
    Slice keySlice = Slices.copyFromUtf8("k1");
    Slice valueSlice = Slices.copyOf(new byte[] {10, 20, 30});
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("topic-1")
            .withKey(keySlice)
            .withValue(valueSlice)
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getKey(), is("k1"));
    assertThat(record.getValueBytes().toByteArray(), is(new byte[] {10, 20, 30}));
  }

  @Test
  public void typedKeyAndValueGoThroughTypeSerializer() throws InvalidProtocolBufferException {
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
        Types.stringType().typeSerializer().deserialize(Slices.wrap(record.getKeyBytes().toByteArray()));
    Integer decodedValue =
        Types.integerType()
            .typeSerializer()
            .deserialize(Slices.wrap(record.getValueBytes().toByteArray()));
    assertThat(decodedKey, is("key-payload"));
    assertThat(decodedValue, is(42));
  }

  @Test
  public void byteValueOverloadIsCoveredByValueOverload() throws InvalidProtocolBufferException {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("t")
            .withValue(new byte[] {7, 8, 9})
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getValueBytes().toByteArray(), is(new byte[] {7, 8, 9}));
  }

  @Test
  public void egressMessageWrapsTypedValueWithKafkaProducerRecordTypename() {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("orders")
            .withUtf8Value("hello")
            .build();

    EgressMessageWrapper wrapper = (EgressMessageWrapper) message;
    String typename = wrapper.typedValue().getTypename();
    assertThat(
        typename,
        is("type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord"));
  }

  @Test
  public void buildWithoutTopicThrows() {
    KafkaEgressMessage.Builder builder =
        KafkaEgressMessage.forEgress(EGRESS_ID).withUtf8Value("v");
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void buildWithoutValueThrows() {
    KafkaEgressMessage.Builder builder =
        KafkaEgressMessage.forEgress(EGRESS_ID).withTopic("t");
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void forEgressRejectsNullEgressId() {
    assertThrows(NullPointerException.class, () -> KafkaEgressMessage.forEgress(null));
  }

  @Test
  public void nullUtf8KeyIsRejected() {
    KafkaEgressMessage.Builder builder = KafkaEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withUtf8Key(null));
  }

  @Test
  public void nullByteKeyIsRejected() {
    KafkaEgressMessage.Builder builder = KafkaEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withKey((byte[]) null));
  }

  @Test
  public void nullSliceKeyIsRejected() {
    KafkaEgressMessage.Builder builder = KafkaEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withKey((Slice) null));
  }

  @Test
  public void nullUtf8ValueIsRejected() {
    KafkaEgressMessage.Builder builder = KafkaEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withUtf8Value(null));
  }

  @Test
  public void nullByteValueIsRejected() {
    KafkaEgressMessage.Builder builder = KafkaEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withValue((byte[]) null));
  }

  @Test
  public void nullSliceValueIsRejected() {
    KafkaEgressMessage.Builder builder = KafkaEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withValue((Slice) null));
  }

  @Test
  public void buildIsValidWithoutKey() throws InvalidProtocolBufferException {
    EgressMessage message =
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic("k-less")
            .withUtf8Value("v")
            .build();

    KafkaProducerRecord record = unpack(message);
    assertThat(record.getKey(), is(""));
  }

  private static KafkaProducerRecord unpack(EgressMessage message)
      throws InvalidProtocolBufferException {
    EgressMessageWrapper wrapper = (EgressMessageWrapper) message;
    return KafkaProducerRecord.parseFrom(wrapper.typedValue().getValue());
  }
}
