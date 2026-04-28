// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.common;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public final class UnimplementedTypeInfo<T> extends TypeInformation<T> {

  private static final long serialVersionUID = 1;

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
  public Class<T> getTypeClass() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isKeyType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeSerializer<T> createSerializer(SerializerConfig serializerConfig) {
    return new UnimplementedTypeSerializer<>();
  }

  @Override
  public String toString() {
    return "UnimplementedTypeInfo";
  }

  @Override
  public boolean equals(Object o) {
    return o == this;
  }

  @Override
  public int hashCode() {
    return 1337;
  }

  @Override
  public boolean canEqual(Object o) {
    return o instanceof UnimplementedTypeInfo;
  }
}
