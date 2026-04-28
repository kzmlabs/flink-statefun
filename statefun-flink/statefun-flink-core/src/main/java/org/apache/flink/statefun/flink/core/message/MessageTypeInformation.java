// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.message;

import java.util.Objects;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class MessageTypeInformation extends TypeInformation<Message> {

  private static final long serialVersionUID = 2L;

  private final MessageFactoryKey messageFactoryKey;

  public MessageTypeInformation(MessageFactoryKey messageFactoryKey) {
    this.messageFactoryKey = Objects.requireNonNull(messageFactoryKey);
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
  public Class<Message> getTypeClass() {
    return Message.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<Message> createSerializer(SerializerConfig serializerConfig) {
    return new MessageTypeSerializer(messageFactoryKey);
  }

  @Override
  public String toString() {
    return String.format(
        "MessageTypeInformation(%s: %s",
        messageFactoryKey.getType(), messageFactoryKey.getCustomPayloadSerializerClassName());
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof MessageTypeInformation;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public boolean canEqual(Object o) {
    return o instanceof MessageTypeInformation;
  }
}
