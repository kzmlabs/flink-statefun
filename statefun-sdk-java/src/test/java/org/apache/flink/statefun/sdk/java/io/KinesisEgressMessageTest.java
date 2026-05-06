// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.io;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.statefun.sdk.egress.generated.KinesisEgressRecord;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageWrapper;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

public class KinesisEgressMessageTest {

  private static final TypeName EGRESS_ID = TypeName.typeNameOf("io.test", "kinesis-egress");

  @Test
  public void utf8RoundtripCarriesStreamPartitionKeyAndValue()
      throws InvalidProtocolBufferException {
    EgressMessage message =
        KinesisEgressMessage.forEgress(EGRESS_ID)
            .withStream("orders")
            .withUtf8PartitionKey("user-1")
            .withUtf8Value("hello")
            .build();

    KinesisEgressRecord record = unpack(message);
    assertThat(message.targetEgressId(), is(EGRESS_ID));
    assertThat(record.getStream(), is("orders"));
    assertThat(record.getPartitionKey(), is("user-1"));
    assertThat(record.getValueBytes().toStringUtf8(), is("hello"));
    assertThat(record.getExplicitHashKey(), is(""));
  }

  @Test
  public void byteValueAndBytePartitionKeyAreCarriedThrough()
      throws InvalidProtocolBufferException {
    byte[] keyBytes = new byte[] {(byte) 'a', (byte) 'b', (byte) 'c'};
    byte[] valueBytes = new byte[] {(byte) 0xff, 0x10, (byte) 0x80};
    EgressMessage message =
        KinesisEgressMessage.forEgress(EGRESS_ID)
            .withStream("events")
            .withPartitionKey(keyBytes)
            .withValue(valueBytes)
            .build();

    KinesisEgressRecord record = unpack(message);
    assertThat(record.getPartitionKeyBytes().toByteArray(), is(keyBytes));
    assertThat(record.getValueBytes().toByteArray(), is(valueBytes));
  }

  @Test
  public void sliceStreamPartitionKeyAndValueAreCopied() throws InvalidProtocolBufferException {
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
    assertThat(record.getStream(), is("dlq"));
    assertThat(record.getPartitionKey(), is("k1"));
    assertThat(record.getValueBytes().toByteArray(), is(new byte[] {7, 8, 9}));
  }

  @Test
  public void typedValueGoesThroughTypeSerializer() throws InvalidProtocolBufferException {
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
    assertThat(decoded, is("payload"));
  }

  @Test
  public void utf8ExplicitHashKeyIsCarriedThrough() throws InvalidProtocolBufferException {
    EgressMessage message =
        KinesisEgressMessage.forEgress(EGRESS_ID)
            .withStream("hashed")
            .withUtf8PartitionKey("p")
            .withUtf8Value("v")
            .withUtf8ExplicitHashKey("0")
            .build();

    KinesisEgressRecord record = unpack(message);
    assertThat(record.getExplicitHashKey(), is("0"));
  }

  @Test
  public void sliceExplicitHashKeyIsCarriedThrough() throws InvalidProtocolBufferException {
    EgressMessage message =
        KinesisEgressMessage.forEgress(EGRESS_ID)
            .withStream("hashed")
            .withUtf8PartitionKey("p")
            .withUtf8Value("v")
            .withUtf8ExplicitHashKey(Slices.copyFromUtf8("explicit"))
            .build();

    KinesisEgressRecord record = unpack(message);
    assertThat(record.getExplicitHashKey(), is("explicit"));
  }

  @Test
  public void egressMessageWrapsTypedValueWithKinesisRecordTypename() {
    EgressMessage message =
        KinesisEgressMessage.forEgress(EGRESS_ID)
            .withStream("orders")
            .withUtf8PartitionKey("p")
            .withUtf8Value("hello")
            .build();

    EgressMessageWrapper wrapper = (EgressMessageWrapper) message;
    String typename = wrapper.typedValue().getTypename();
    assertThat(
        typename,
        is("type.googleapis.com/io.statefun.sdk.egress.KinesisEgressRecord"));
  }

  @Test
  public void buildWithoutStreamThrows() {
    KinesisEgressMessage.Builder builder =
        KinesisEgressMessage.forEgress(EGRESS_ID).withUtf8PartitionKey("p").withUtf8Value("v");
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void buildWithoutPartitionKeyThrows() {
    KinesisEgressMessage.Builder builder =
        KinesisEgressMessage.forEgress(EGRESS_ID).withStream("s").withUtf8Value("v");
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void buildWithoutValueThrows() {
    KinesisEgressMessage.Builder builder =
        KinesisEgressMessage.forEgress(EGRESS_ID).withStream("s").withUtf8PartitionKey("p");
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void forEgressRejectsNullEgressId() {
    assertThrows(NullPointerException.class, () -> KinesisEgressMessage.forEgress(null));
  }

  @Test
  public void nullStreamUtf8IsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withStream((String) null));
  }

  @Test
  public void nullUtf8PartitionKeyIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withUtf8PartitionKey(null));
  }

  @Test
  public void nullBytePartitionKeyIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withPartitionKey((byte[]) null));
  }

  @Test
  public void nullSlicePartitionKeyIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withPartitionKey((Slice) null));
  }

  @Test
  public void nullUtf8ValueIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withUtf8Value(null));
  }

  @Test
  public void nullByteValueIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withValue((byte[]) null));
  }

  @Test
  public void nullSliceValueIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withValue((Slice) null));
  }

  @Test
  public void nullUtf8ExplicitHashKeyStringIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withUtf8ExplicitHashKey((String) null));
  }

  @Test
  public void nullUtf8ExplicitHashKeySliceIsRejected() {
    KinesisEgressMessage.Builder builder = KinesisEgressMessage.forEgress(EGRESS_ID);
    assertThrows(NullPointerException.class, () -> builder.withUtf8ExplicitHashKey((Slice) null));
  }

  private static KinesisEgressRecord unpack(EgressMessage message)
      throws InvalidProtocolBufferException {
    EgressMessageWrapper wrapper = (EgressMessageWrapper) message;
    return KinesisEgressRecord.parseFrom(wrapper.typedValue().getValue());
  }
}
