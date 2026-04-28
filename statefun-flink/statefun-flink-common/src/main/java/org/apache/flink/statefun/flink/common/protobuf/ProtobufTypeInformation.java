// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.common.protobuf;

import com.google.protobuf.Message;
import java.util.Objects;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class ProtobufTypeInformation<M extends Message> extends TypeInformation<M> {

  private static final long serialVersionUID = 1L;

  private final Class<M> messageTypeClass;

  public ProtobufTypeInformation(Class<M> messageTypeClass) {
    this.messageTypeClass = Objects.requireNonNull(messageTypeClass);
  }

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 0;
  }

  @Override
  public int getTotalFields() {
    return 0;
  }

  @Override
  public Class<M> getTypeClass() {
    return messageTypeClass;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<M> createSerializer(SerializerConfig config) {
    return new ProtobufTypeSerializer<>(messageTypeClass);
  }

  @Override
  public String toString() {
    return "ProtobufTypeInformation(" + messageTypeClass + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProtobufTypeInformation<?> that = (ProtobufTypeInformation<?>) o;
    return messageTypeClass.equals(that.messageTypeClass);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(messageTypeClass);
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof ProtobufTypeInformation;
  }
}
