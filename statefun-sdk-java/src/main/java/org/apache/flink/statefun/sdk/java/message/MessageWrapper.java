// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.java.message;

import java.util.List;
import java.util.Objects;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.annotations.Internal;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.TypeSerializer;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

@Internal
public final class MessageWrapper implements Message {
  private final TypedValue typedValue;
  private final Address targetAddress;
  private List<MessageHeader> headers;

  public MessageWrapper(Address targetAddress, TypedValue typedValue) {
    this.targetAddress = Objects.requireNonNull(targetAddress);

    if (!typedValue.getHasValue()) {
      throw new IllegalStateException("Unset empty Messages are prohibited.");
    }
    this.typedValue = Objects.requireNonNull(typedValue);
  }

  @Override
  public Address targetAddress() {
    return targetAddress;
  }

  @Override
  public List<MessageHeader> headers() {
    List<MessageHeader> headers = this.headers;
    if (headers == null) {
      this.headers = headers = extractHeaders(typedValue);
    }
    return headers;
  }

  private static List<MessageHeader> extractHeaders(TypedValue typedValue) {
    if (typedValue.getMetadataCount() == 0) {
      return List.of();
    }
    return typedValue.getMetadataList().stream()
        .map(
            metadata ->
                new MessageHeader(
                    metadata.getKey(),
                    metadata.getHasValue() ? SliceProtobufUtil.asSlice(metadata.getValue()) : null))
        .toList();
  }

  @Override
  public boolean isLong() {
    return is(Types.longType());
  }

  @Override
  public long asLong() {
    return as(Types.longType());
  }

  @Override
  public boolean isUtf8String() {
    return is(Types.stringType());
  }

  @Override
  public String asUtf8String() {
    return as(Types.stringType());
  }

  @Override
  public boolean isInt() {
    return is(Types.integerType());
  }

  @Override
  public int asInt() {
    return as(Types.integerType());
  }

  @Override
  public boolean isBoolean() {
    return is(Types.booleanType());
  }

  @Override
  public boolean asBoolean() {
    return as(Types.booleanType());
  }

  @Override
  public boolean isFloat() {
    return is(Types.floatType());
  }

  @Override
  public float asFloat() {
    return as(Types.floatType());
  }

  @Override
  public boolean isDouble() {
    return is(Types.doubleType());
  }

  @Override
  public double asDouble() {
    return as(Types.doubleType());
  }

  @Override
  public <T> boolean is(Type<T> type) {
    String thisTypeNameString = typedValue.getTypename();
    String thatTypeNameString = type.typeName().asTypeNameString();
    return thisTypeNameString.equals(thatTypeNameString);
  }

  @Override
  public <T> T as(Type<T> type) {
    TypeSerializer<T> typeSerializer = type.typeSerializer();
    Slice input = SliceProtobufUtil.asSlice(typedValue.getValue());
    return typeSerializer.deserialize(input);
  }

  @Override
  public TypeName valueTypeName() {
    return TypeName.typeNameFromString(typedValue.getTypename());
  }

  @Override
  public Slice rawValue() {
    return SliceProtobufUtil.asSlice(typedValue.getValue());
  }

  public TypedValue typedValue() {
    return typedValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MessageWrapper that = (MessageWrapper) o;
    return Objects.equals(typedValue, that.typedValue)
        && Objects.equals(targetAddress, that.targetAddress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typedValue, targetAddress);
  }

  @Override
  public String toString() {
    return "MessageWrapper{"
        + "typedValue="
        + typedValue
        + ", targetAddress="
        + targetAddress
        + '}';
  }
}
