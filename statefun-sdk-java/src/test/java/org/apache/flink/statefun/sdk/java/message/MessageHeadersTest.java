// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

/**
 * Pins the ingress-header contract of {@link Message#headers()}: entries of {@code
 * TypedValue.metadata} (populated by the runtime from Kafka record headers) are exposed as {@link
 * MessageHeader}s in order, and messages without metadata expose an empty, allocation-free list.
 */
class MessageHeadersTest {

  private static final Address TARGET =
      new Address(TypeName.typeNameOf("io.test", "fn"), "id-1");

  @Test
  void metadataEntriesAreExposedAsHeadersInOrder() {
    TypedValue typedValue =
        plainStringValue()
            .addMetadata(metadata("trace-id", "abc-123"))
            .addMetadata(metadata("origin", "gateway"))
            .addMetadata(metadata("trace-id", "def-456"))
            .build();

    Message message = new MessageWrapper(TARGET, typedValue);

    List<MessageHeader> headers = message.headers();
    assertThat(headers).hasSize(3);
    assertThat(headers.get(0).key()).isEqualTo("trace-id");
    assertThat(headers.get(0).valueAsUtf8String()).isEqualTo("abc-123");
    assertThat(headers.get(1).key()).isEqualTo("origin");
    assertThat(headers.get(1).valueAsUtf8String()).isEqualTo("gateway");
    assertThat(headers.get(2).key()).isEqualTo("trace-id");
    assertThat(headers.get(2).valueAsUtf8String()).isEqualTo("def-456");
  }

  @Test
  void binaryHeaderValueIsAccessibleAsSlice() {
    byte[] raw = new byte[] {0, 1, (byte) 0xFF};
    TypedValue typedValue =
        plainStringValue()
            .addMetadata(
                TypedValue.Metadata.newBuilder()
                    .setKey("bin")
                    .setValue(ByteString.copyFrom(raw))
                    .setHasValue(true))
            .build();

    Message message = new MessageWrapper(TARGET, typedValue);

    assertThat(message.headers().get(0).value().toByteArray()).containsExactly(0, 1, 0xFF);
  }

  @Test
  void headerValuesDecodeAsBytesStringAndTypedPrimitives() {
    TypedValue typedValue =
        plainStringValue()
            .addMetadata(
                TypedValue.Metadata.newBuilder()
                    .setKey("raw-bytes")
                    .setValue(ByteString.copyFrom(new byte[] {1, 2, 3}))
                    .setHasValue(true))
            .addMetadata(metadata("str", "hello"))
            .addMetadata(typedMetadata("int", Types.integerType(), 42))
            .addMetadata(typedMetadata("long", Types.longType(), 42_000_000_000L))
            .addMetadata(typedMetadata("double", Types.doubleType(), 3.14d))
            .addMetadata(typedMetadata("bool", Types.booleanType(), true))
            .build();

    List<MessageHeader> headers = new MessageWrapper(TARGET, typedValue).headers();

    assertThat(headers.get(0).value().toByteArray()).containsExactly(1, 2, 3);
    assertThat(headers.get(1).valueAsUtf8String()).isEqualTo("hello");
    assertThat(headers.get(2).valueAs(Types.integerType())).isEqualTo(42);
    assertThat(headers.get(3).valueAs(Types.longType())).isEqualTo(42_000_000_000L);
    assertThat(headers.get(4).valueAs(Types.doubleType())).isEqualTo(3.14d);
    assertThat(headers.get(5).valueAs(Types.booleanType())).isTrue();
  }

  @Test
  void messageWithoutMetadataHasNoHeaders() {
    Message message = new MessageWrapper(TARGET, plainStringValue().build());

    assertThat(message.headers()).isEmpty();
  }

  @Test
  void headersListIsUnmodifiable() {
    TypedValue typedValue = plainStringValue().addMetadata(metadata("k", "v")).build();
    Message message = new MessageWrapper(TARGET, typedValue);

    List<MessageHeader> headers = message.headers();
    assertThatThrownBy(() -> headers.add(headers.get(0)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void messageHeaderNeverThrowsOnDegenerateInput() {
    MessageHeader header = new MessageHeader(null, null);

    assertThat(header.key()).isEmpty();
    assertThat(header.hasValue()).isFalse();
    assertThat(header.value()).isNull();
    assertThat(header.valueAsUtf8String()).isNull();
    assertThat(header.valueAs(Types.integerType())).isNull();
  }

  @Test
  void nullValuedKafkaHeaderIsPreservedAsNullValue() {
    TypedValue typedValue =
        plainStringValue()
            .addMetadata(TypedValue.Metadata.newBuilder().setKey("null-header"))
            .addMetadata(metadata("present", "x"))
            .build();

    List<MessageHeader> headers = new MessageWrapper(TARGET, typedValue).headers();

    assertThat(headers.get(0).key()).isEqualTo("null-header");
    assertThat(headers.get(0).hasValue()).isFalse();
    assertThat(headers.get(0).value()).isNull();
    assertThat(headers.get(1).hasValue()).isTrue();
    assertThat(headers.get(1).valueAsUtf8String()).isEqualTo("x");
  }

  @Test
  void headersAreComputedOnceAndCached() {
    TypedValue typedValue = plainStringValue().addMetadata(metadata("k", "v")).build();
    Message message = new MessageWrapper(TARGET, typedValue);

    assertThat(message.headers()).isSameAs(message.headers());
  }

  private static TypedValue.Builder plainStringValue() {
    return TypedValue.newBuilder()
        .setTypename("io.statefun.types/string")
        .setHasValue(true)
        .setValue(ByteString.copyFromUtf8("payload"));
  }

  private static TypedValue.Metadata.Builder metadata(String key, String utf8Value) {
    return TypedValue.Metadata.newBuilder()
        .setKey(key)
        .setValue(ByteString.copyFromUtf8(utf8Value))
        .setHasValue(true);
  }

  private static <T> TypedValue.Metadata.Builder typedMetadata(
      String key, org.apache.flink.statefun.sdk.java.types.Type<T> type, T value) {
    return TypedValue.Metadata.newBuilder()
        .setKey(key)
        .setValue(ByteString.copyFrom(type.typeSerializer().serialize(value).toByteArray()))
        .setHasValue(true);
  }
}
