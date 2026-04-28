// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.java.message;

import java.util.Objects;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.annotations.Internal;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

@Internal
public final class EgressMessageWrapper implements EgressMessage {
  private final TypedValue typedValue;
  private final TypeName targetEgressId;

  public EgressMessageWrapper(TypeName targetEgressId, TypedValue actualMessage) {
    this.targetEgressId = Objects.requireNonNull(targetEgressId);
    this.typedValue = Objects.requireNonNull(actualMessage);
  }

  @Override
  public TypeName targetEgressId() {
    return targetEgressId;
  }

  @Override
  public TypeName egressMessageValueType() {
    return TypeName.typeNameFromString(typedValue.getTypename());
  }

  @Override
  public Slice egressMessageValueBytes() {
    return SliceProtobufUtil.asSlice(typedValue.getValue());
  }

  public TypedValue typedValue() {
    return typedValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EgressMessageWrapper that = (EgressMessageWrapper) o;
    return Objects.equals(typedValue, that.typedValue)
        && Objects.equals(targetEgressId, that.targetEgressId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typedValue, targetEgressId);
  }

  @Override
  public String toString() {
    return "EgressMessageWrapper{"
        + "typedValue="
        + typedValue
        + ", targetEgressId="
        + targetEgressId
        + '}';
  }
}
