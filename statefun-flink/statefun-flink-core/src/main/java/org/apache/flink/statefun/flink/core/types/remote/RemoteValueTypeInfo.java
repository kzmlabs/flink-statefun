// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.types.remote;

import java.util.Objects;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.statefun.sdk.TypeName;

public final class RemoteValueTypeInfo extends TypeInformation<byte[]> {

  private static final long serialVersionUID = 1L;

  private final TypeName type;

  public RemoteValueTypeInfo(TypeName type) {
    this.type = Objects.requireNonNull(type);
  }

  @Override
  public TypeSerializer<byte[]> createSerializer(SerializerConfig serializerConfig) {
    return new RemoteValueSerializer(type);
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
  public Class<byte[]> getTypeClass() {
    return byte[].class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public String toString() {
    return "RemoteValueTypeInfo{" + "type=" + type + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RemoteValueTypeInfo that = (RemoteValueTypeInfo) o;
    return type.equals(that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type);
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof RemoteValueTypeInfo;
  }
}
