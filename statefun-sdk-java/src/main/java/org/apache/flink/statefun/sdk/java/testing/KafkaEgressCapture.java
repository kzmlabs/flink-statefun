// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.testing;

import java.util.List;
import java.util.Optional;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageWrapper;
import org.apache.flink.statefun.sdk.java.message.MessageHeader;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.InvalidProtocolBufferException;

/**
 * A readable view over a captured Kafka {@link EgressMessage}, for asserting in tests what a
 * function wrote to a Kafka egress — topic, key, value and headers — without hand-parsing the
 * underlying {@code KafkaProducerRecord} protobuf.
 *
 * <pre>{@code
 * TestContext context = TestContext.forTarget(SELF);
 * functionUnderTest.apply(context, message);
 *
 * KafkaEgressCapture record =
 *     KafkaEgressCapture.of(context.getSentEgressMessages().get(0).message());
 * assertThat(record.topic()).isEqualTo("orders");
 * assertThat(record.lastHeader("retry-count").orElseThrow().valueAsInt()).isEqualTo(10);
 * }</pre>
 *
 * <p>Unlike production header paths, this test helper fails loudly: passing a non-Kafka egress
 * message throws {@link IllegalArgumentException}.
 */
public final class KafkaEgressCapture {

  private final TypeName targetEgressId;
  private final KafkaProducerRecord record;
  private final List<MessageHeader> headers;

  private KafkaEgressCapture(TypeName targetEgressId, KafkaProducerRecord record) {
    this.targetEgressId = targetEgressId;
    this.record = record;
    this.headers =
        record.getHeadersList().stream()
            .map(
                header ->
                    new MessageHeader(
                        header.getKey(),
                        header.getHasValue()
                            ? SliceProtobufUtil.asSlice(header.getValue())
                            : null))
            .toList();
  }

  private static final String KAFKA_PRODUCER_RECORD_TYPENAME =
      "type.googleapis.com/" + KafkaProducerRecord.getDescriptor().getFullName();

  public static KafkaEgressCapture of(EgressMessage message) {
    if (!(message instanceof EgressMessageWrapper wrapper)) {
      throw new IllegalArgumentException(
          "Expected an SDK-built egress message, got: " + message.getClass().getName());
    }
    if (!KAFKA_PRODUCER_RECORD_TYPENAME.equals(wrapper.typedValue().getTypename())) {
      throw new IllegalArgumentException(
          "The egress message does not carry a KafkaProducerRecord (typename was "
              + wrapper.typedValue().getTypename()
              + ") — was it built with KafkaEgressMessage.forEgress(...)?");
    }
    try {
      return new KafkaEgressCapture(
          message.targetEgressId(), KafkaProducerRecord.parseFrom(wrapper.typedValue().getValue()));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Corrupted KafkaProducerRecord bytes.", e);
    }
  }

  public TypeName targetEgressId() {
    return targetEgressId;
  }

  public String topic() {
    return record.getTopic();
  }

  public String utf8Key() {
    return record.getKey();
  }

  public Slice value() {
    return SliceProtobufUtil.asSlice(record.getValueBytes());
  }

  public String utf8Value() {
    return record.getValueBytes().toStringUtf8();
  }

  /** All record headers in write order; null-valued headers surface with {@code hasValue()==false}. */
  public List<MessageHeader> headers() {
    return headers;
  }

  /** The last header with the given key, mirroring Kafka's {@code Headers#lastHeader} semantics. */
  public Optional<MessageHeader> lastHeader(String key) {
    return headers.stream().filter(header -> header.key().equals(key)).reduce((a, b) -> b);
  }
}
