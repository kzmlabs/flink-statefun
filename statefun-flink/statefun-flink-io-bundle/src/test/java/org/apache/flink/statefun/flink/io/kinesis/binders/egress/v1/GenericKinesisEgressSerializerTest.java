// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.io.kinesis.binders.egress.v1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import org.apache.flink.statefun.flink.common.types.TypedValueUtil;
import org.apache.flink.statefun.sdk.egress.generated.KinesisEgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.EgressRecord;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.jupiter.api.Test;

class GenericKinesisEgressSerializerTest {

  private final GenericKinesisEgressSerializer serializer = new GenericKinesisEgressSerializer();

  @Test
  void serializesRecordWithoutExplicitHashKey() {
    TypedValue input = pack("stream-A", "pk-1", null, "value".getBytes(StandardCharsets.UTF_8));

    EgressRecord rec = serializer.serialize(input);

    assertThat(rec.getStream()).isEqualTo("stream-A");
    assertThat(rec.getPartitionKey()).isEqualTo("pk-1");
    assertThat(rec.getData()).isEqualTo("value".getBytes(StandardCharsets.UTF_8));
    assertThat(rec.getExplicitHashKey()).isNull();
  }

  @Test
  void serializesRecordWithExplicitHashKey() {
    TypedValue input =
        pack("stream-A", "pk-1", "0123456789abcdef", "v".getBytes(StandardCharsets.UTF_8));

    EgressRecord rec = serializer.serialize(input);

    assertThat(rec.getExplicitHashKey()).isEqualTo("0123456789abcdef");
  }

  @Test
  void emptyExplicitHashKeyIsTreatedAsAbsent() {
    // The proto field `explicit_hash_key` defaults to "" when unset. The serializer should
    // treat that as "not provided" — passing "" to the Kinesis SDK has different semantics
    // than omitting it.
    TypedValue input = pack("stream-A", "pk", "", "v".getBytes(StandardCharsets.UTF_8));

    EgressRecord rec = serializer.serialize(input);

    assertThat(rec.getExplicitHashKey()).isNull();
  }

  @Test
  void rejectsTypedValueOfWrongProtobufType() {
    TypedValue notAKinesisRecord =
        TypedValueUtil.packProtobufMessage(TypedValue.getDefaultInstance());

    assertThatThrownBy(() -> serializer.serialize(notAKinesisRecord))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("KinesisEgressRecord");
  }

  @Test
  void rejectsCorruptedProtobufBytes() {
    TypedValue corrupted =
        TypedValue.newBuilder()
            .setTypename("type.googleapis.com/" + KinesisEgressRecord.getDescriptor().getFullName())
            .setHasValue(true)
            .setValue(ByteString.copyFrom(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}))
            .build();

    assertThatThrownBy(() -> serializer.serialize(corrupted))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Unable to unpack");
  }

  @Test
  void serializerIsSerializableForFlinkOperatorChain() throws Exception {
    java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
    new java.io.ObjectOutputStream(bytes).writeObject(serializer);

    Object copy =
        new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray()))
            .readObject();

    assertThat(copy).isInstanceOf(GenericKinesisEgressSerializer.class);
  }

  private static TypedValue pack(String stream, String pk, String explicitHashKey, byte[] value) {
    KinesisEgressRecord.Builder b =
        KinesisEgressRecord.newBuilder()
            .setStream(stream)
            .setPartitionKey(pk)
            .setValueBytes(ByteString.copyFrom(value));
    if (explicitHashKey != null) {
      b.setExplicitHashKey(explicitHashKey);
    }
    return TypedValueUtil.packProtobufMessage(b.build());
  }
}
