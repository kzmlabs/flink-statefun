// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import org.apache.flink.statefun.flink.core.generated.Payload;
import org.junit.jupiter.api.Test;

class MessagePayloadSerializerPbTest {

  private final MessagePayloadSerializerPb serializer = new MessagePayloadSerializerPb();
  private final ClassLoader classLoader = getClass().getClassLoader();

  @Test
  void roundtripPreservesProtobufMessage() {
    Payload original =
        Payload.newBuilder()
            .setClassName("com.example.Original")
            .setPayloadBytes(ByteString.copyFromUtf8("hello"))
            .build();

    Payload serialized = serializer.serialize(original);

    // Pin the wire shape: the className field carries the Java class FQN, the payload bytes
    // carry the original message's byte form. This is load-bearing for cross-classloader
    // deserialization in StateFun.
    assertThat(serialized.getClassName()).isEqualTo(Payload.class.getName());

    Object deserialized = serializer.deserialize(classLoader, serialized);
    assertThat(deserialized).isInstanceOf(Payload.class);
    assertThat(((Payload) deserialized).getPayloadBytes().toStringUtf8()).isEqualTo("hello");
  }

  @Test
  void parserCacheReusesSameParserOnSecondCall() {
    Payload original =
        Payload.newBuilder()
            .setClassName("com.example.Original")
            .setPayloadBytes(ByteString.copyFromUtf8("first"))
            .build();
    Payload original2 =
        Payload.newBuilder()
            .setClassName("com.example.Original")
            .setPayloadBytes(ByteString.copyFromUtf8("second"))
            .build();

    Payload serialized = serializer.serialize(original);
    Payload serialized2 = serializer.serialize(original2);

    // Two deserializations of the same message class should both succeed (exercises the cache hit
    // path that fastutil's ObjectOpenHashMap-backed cache produces).
    Object first = serializer.deserialize(classLoader, serialized);
    Object second = serializer.deserialize(classLoader, serialized2);

    assertThat(((Payload) first).getPayloadBytes().toStringUtf8()).isEqualTo("first");
    assertThat(((Payload) second).getPayloadBytes().toStringUtf8()).isEqualTo("second");
  }

  @Test
  void copyProducesIndependentInstanceWithEqualContent() {
    Payload original =
        Payload.newBuilder()
            .setClassName(Payload.class.getName())
            .setPayloadBytes(ByteString.copyFromUtf8("value"))
            .build();

    Object copy = serializer.copy(classLoader, original);

    assertThat(copy).isNotSameAs(original).isEqualTo(original);
  }

  @Test
  void copyRejectsNonProtobufObject() {
    assertThatThrownBy(() -> serializer.copy(classLoader, "not-a-proto"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void deserializeUnknownClassNameThrowsIllegalState() {
    Payload corrupt =
        Payload.newBuilder()
            .setClassName("does.not.exist.SomeClass")
            .setPayloadBytes(ByteString.EMPTY)
            .build();

    assertThatThrownBy(() -> serializer.deserialize(classLoader, corrupt))
        .isInstanceOf(IllegalStateException.class)
        .hasCauseInstanceOf(ClassNotFoundException.class);
  }

  @Test
  void deserializeMalformedBytesThrowsIllegalState() {
    Payload corrupt =
        Payload.newBuilder()
            .setClassName(Payload.class.getName())
            .setPayloadBytes(ByteString.copyFrom(new byte[] {1, 2, 3, 4, 5})) // not valid wire form
            .build();

    assertThatThrownBy(() -> serializer.deserialize(classLoader, corrupt))
        .isInstanceOf(IllegalStateException.class);
  }
}
