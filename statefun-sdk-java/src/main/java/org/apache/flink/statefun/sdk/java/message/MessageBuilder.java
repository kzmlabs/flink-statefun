// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.java.message;

import java.util.Objects;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.ApiExtension;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.TypeSerializer;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;

public final class MessageBuilder {
  private final TypedValue.Builder builder;
  private Address targetAddress;

  private MessageBuilder(TypeName functionType, String id) {
    this(functionType, id, TypedValue.newBuilder());
  }

  private MessageBuilder(TypeName functionType, String id, TypedValue.Builder builder) {
    this.targetAddress = new Address(functionType, id);
    this.builder = Objects.requireNonNull(builder);
  }

  public static MessageBuilder forAddress(TypeName functionType, String id) {
    return new MessageBuilder(functionType, id);
  }

  public static MessageBuilder forAddress(Address address) {
    Objects.requireNonNull(address);
    return new MessageBuilder(address.type(), address.id());
  }

  public static MessageBuilder fromMessage(Message message) {
    Address targetAddress = message.targetAddress();
    TypedValue.Builder builder = typedValueBuilder(message);
    return new MessageBuilder(targetAddress.type(), targetAddress.id(), builder);
  }

  public MessageBuilder withValue(long value) {
    return withCustomType(Types.longType(), value);
  }

  public MessageBuilder withValue(int value) {
    return withCustomType(Types.integerType(), value);
  }

  public MessageBuilder withValue(boolean value) {
    return withCustomType(Types.booleanType(), value);
  }

  public MessageBuilder withValue(String value) {
    return withCustomType(Types.stringType(), value);
  }

  public MessageBuilder withValue(float value) {
    return withCustomType(Types.floatType(), value);
  }

  public MessageBuilder withValue(double value) {
    return withCustomType(Types.doubleType(), value);
  }

  public MessageBuilder withTargetAddress(Address targetAddress) {
    this.targetAddress = Objects.requireNonNull(targetAddress);
    return this;
  }

  public MessageBuilder withTargetAddress(TypeName typeName, String id) {
    return withTargetAddress(new Address(typeName, id));
  }

  /**
   * Attaches a transport-level header to the built message, readable via {@link
   * Message#headers()} — the primary way to construct header-carrying messages in tests. Same
   * never-throw contract as the Kafka egress builder: a null value is preserved as a null-valued
   * header, a null key degrades to an empty key.
   */
  public MessageBuilder withUtf8Header(String key, String value) {
    return addHeader(key, value == null ? null : ByteString.copyFromUtf8(value));
  }

  public MessageBuilder withHeader(String key, byte[] value) {
    return addHeader(key, value == null ? null : ByteString.copyFrom(value));
  }

  public MessageBuilder withHeader(String key, Slice value) {
    return addHeader(key, value == null ? null : SliceProtobufUtil.asByteString(value));
  }

  public <T> MessageBuilder withHeader(String key, Type<T> type, T value) {
    if (type == null || value == null) {
      return addHeader(key, null);
    }
    return withHeader(key, type.typeSerializer().serialize(value));
  }

  public MessageBuilder withHeader(String key, int value) {
    return withHeader(key, Types.integerType(), value);
  }

  public MessageBuilder withHeader(String key, long value) {
    return withHeader(key, Types.longType(), value);
  }

  public MessageBuilder withHeader(String key, float value) {
    return withHeader(key, Types.floatType(), value);
  }

  public MessageBuilder withHeader(String key, double value) {
    return withHeader(key, Types.doubleType(), value);
  }

  public MessageBuilder withHeader(String key, boolean value) {
    return withHeader(key, Types.booleanType(), value);
  }

  private MessageBuilder addHeader(String key, ByteString valueOrNull) {
    TypedValue.Metadata.Builder metadata =
        TypedValue.Metadata.newBuilder().setKey(key == null ? "" : key);
    if (valueOrNull != null) {
      metadata.setValue(valueOrNull).setHasValue(true);
    }
    builder.addMetadata(metadata);
    return this;
  }

  public <T> MessageBuilder withCustomType(Type<T> customType, T element) {
    Objects.requireNonNull(customType);
    Objects.requireNonNull(element);
    TypeSerializer<T> typeSerializer = customType.typeSerializer();
    builder.setTypenameBytes(ApiExtension.typeNameByteString(customType.typeName()));
    Slice serialized = typeSerializer.serialize(element);
    ByteString serializedByteString = SliceProtobufUtil.asByteString(serialized);
    builder.setValue(serializedByteString);
    builder.setHasValue(true);
    return this;
  }

  public Message build() {
    return new MessageWrapper(targetAddress, builder.build());
  }

  private static TypedValue.Builder typedValueBuilder(Message message) {
    ByteString typenameBytes = ApiExtension.typeNameByteString(message.valueTypeName());
    ByteString valueBytes = SliceProtobufUtil.asByteString(message.rawValue());
    TypedValue.Builder typedValue =
        TypedValue.newBuilder()
            .setTypenameBytes(typenameBytes)
            .setHasValue(true)
            .setValue(valueBytes);
    message.headers().forEach(header -> typedValue.addMetadata(asMetadata(header)));
    return typedValue;
  }

  private static TypedValue.Metadata asMetadata(MessageHeader header) {
    TypedValue.Metadata.Builder metadata = TypedValue.Metadata.newBuilder().setKey(header.key());
    if (header.hasValue()) {
      metadata.setValue(SliceProtobufUtil.asByteString(header.value())).setHasValue(true);
    }
    return metadata.build();
  }
}
