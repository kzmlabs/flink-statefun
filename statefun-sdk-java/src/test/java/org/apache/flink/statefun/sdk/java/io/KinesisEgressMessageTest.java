// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.statefun.sdk.egress.generated.KinesisEgressRecord;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageWrapper;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

class KinesisEgressMessageTest {

  private static final TypeName EGRESS_ID = TypeName.typeNameOf("io.test", "kinesis-egress");

  @Test
  void utf8RoundtripCarriesStreamPartitionKeyAndValue() throws InvalidProtocolBufferException {
    EgressMessage message =
        KinesisEgressMessage.forEgress(EGRESS_ID)
            .withStream("orders")
            .withUtf8PartitionKey("user-1")
            .withUtf8Value("hello")
            .build();

    KinesisEgressRecord record = unpack(message);
    assertThat(message.targetEgressId()).isEqualTo(EGRESS_ID);
    assertThat(record.getStream()).isEqualTo("orders");
    assertThat(record.getPartitionKey()).isEqualTo("user-1");
    assertThat(record.getValueBytes().toStringUtf8()).isEqualTo("hello");
    assertThat(record.getExplicitHashKey()).isEmpty();
  }

  @Test
  void byteValueAndBytePartitionKeyAreCarriedThrough() throws InvalidProtocolBufferException {
    byte[] keyBytes = new byte[] {(byte) 'a', (byte) 'b', (byte) 'c'};
    byte[] valueBytes = new byte[] {(byte) 0xff, 0x10, (byte) 0x80};
    EgressMessage message =
        KinesisEgressMessage.forEgress(EGRESS_ID)
            .withStream("events")
            .withPartitionKey(keyBytes)
            .withValue(valueBytes)
            .build();

    KinesisEgressRecord record = unpack(message);
    assertThat(record.getPartitionKeyBytes().toByteArray()).containsExactly(keyBytes);
    assertThat(record.getValueBytes().toByteArray()).containsExactly(valueBytes);
  }

  @Test
  void sliceStreamPartitionKeyAndValueAreCopied() throws InvalidProtocolBufferException {
    Slice streamSlice = Slices.copyFromUtf8("dlq");
    Slice keySlice = Slices.copyFromUtf8("k1");
    Slice valueSlice = Slices.copyOf(new byte[] {7, 8, 9});

    EgressMessage message =
        KinesisEgressMessage.forEgress(EGRESS_ID)
            .withStream(streamSlice)
            .withPartitionKey(keySlice)
            .withValue(valueSlice)
            .build();

    KinesisEgressRecord record = unpack(message);
    assertThat(record.getStream()).isEqualTo("dlq");
    assertThat(record.getPartitionKey()).isEqualTo("k1");
    assertThat(record.getValueBytes().toByteArray()).containsExactly(7, 8, 9);
  }

  @Test
  void typedValueGoesThroughTypeSerializer() throws InvalidProtocolBufferException {
    EgressMessage message =
        KinesisEgressMessage.forEgress(EGRESS_ID)
            .withStream("typed")
            .withUtf8PartitionKey("p")
            .withValue(Types.stringType(), "payload")
            .build();

    KinesisEgressRecord record = unpack(message);
    String decoded =
        Types.stringType()
            .typeSerializer()
            .deserialize(Slices.wrap(record.getValueBytes().toByteArray()));
    assertThat(decoded).isEqualTo("payload");
  }

  @Test
  void utf8ExplicitHashKeyIsCarriedThrough() throws InvalidProtocolBufferException {
    EgressMessage message =
        KinesisEgressMessage.forEgress(EGRESS_ID)
            .withStream("hashed")
            .withUtf8PartitionKey("p")
            .withUtf8Value("v")
            .withUtf8ExplicitHashKey("0")
            .build();

    KinesisEgressRecord record = unpack(message);
    assertThat(record.getExplicitHashKey()).isEqualTo("0");
  }

  @Test
  void sliceExplicitHashKeyIsCarriedThrough() throws InvalidProtocolBufferException {
    EgressMessage message =
        KinesisEgressMessage.forEgress(EGRESS_ID)
            .withStream("hashed")
            .withUtf8PartitionKey("p")
            .withUtf8Value("v")
            .withUtf8ExplicitHashKey(Slices.copyFromUtf8("explicit"))
            .build();

    KinesisEgressRecord record = unpack(message);
    assertThat(record.getExplicitHashKey()).isEqualTo("explicit");
  }

  @Test
  void egressMessageWrapsTypedValueWithKinesisRecordTypename() {
    EgressMessage message =
        KinesisEgressMessage.forEgress(EGRESS_ID)
            .withStream("orders")
            .withUtf8PartitionKey("p")
            .withUtf8Value("hello")
            .build();

    EgressMessageWrapper wrapper = (EgressMessageWrapper) message;
    assertThat(wrapper.typedValue().getTypename())
        .isEqualTo("type.googleapis.com/io.statefun.sdk.egress.KinesisEgressRecord");
  }

  @Test
  void buildWithoutStreamThrows() {
    KinesisEgressMessage.Builder builder =
        KinesisEgressMessage.forEgress(EGRESS_ID).withUtf8PartitionKey("p").withUtf8Value("v");
    assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void buildWithoutPartitionKeyThrows() {
    KinesisEgressMessage.Builder builder =
        KinesisEgressMessage.forEgress(EGRESS_ID).withStream("s").withUtf8Value("v");
    assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void buildWithoutValueThrows() {
    KinesisEgressMessage.Builder builder =
        KinesisEgressMessage.forEgress(EGRESS_ID).withStream("s").withUtf8PartitionKey("p");
    assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void forEgressRejectsNullEgressId() {
    assertThatThrownBy(() -> KinesisEgressMessage.forEgress(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullStreamUtf8IsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withStream((String) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullUtf8PartitionKeyIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withUtf8PartitionKey(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullBytePartitionKeyIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withPartitionKey((byte[]) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullSlicePartitionKeyIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withPartitionKey((Slice) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullUtf8ValueIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withUtf8Value(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullByteValueIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withValue((byte[]) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullSliceValueIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withValue((Slice) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullUtf8ExplicitHashKeyStringIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withUtf8ExplicitHashKey((String) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullUtf8ExplicitHashKeySliceIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThatThrownBy(() -> builder.withUtf8ExplicitHashKey((Slice) null))
        .isInstanceOf(NullPointerException.class);
  }

  private static KinesisEgressRecord unpack(EgressMessage message)
      throws InvalidProtocolBufferException {
    EgressMessageWrapper wrapper = (EgressMessageWrapper) message;
    return KinesisEgressRecord.parseFrom(wrapper.typedValue().getValue());
  }
}
