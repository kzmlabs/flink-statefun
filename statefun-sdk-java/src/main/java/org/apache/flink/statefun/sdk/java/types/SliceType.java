// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.sdk.java.types;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.slice.Slice;

public final class SliceType implements Type<Slice> {

  private static final Set<TypeCharacteristics> IMMUTABLE_TYPE_CHARS =
      Collections.unmodifiableSet(EnumSet.of(TypeCharacteristics.IMMUTABLE_VALUES));

  private final TypeName typename;
  private final Serializer serializer = new Serializer();

  public SliceType(TypeName typename) {
    this.typename = Objects.requireNonNull(typename);
  }

  @Override
  public TypeName typeName() {
    return typename;
  }

  @Override
  public TypeSerializer<Slice> typeSerializer() {
    return serializer;
  }

  @Override
  public Set<TypeCharacteristics> typeCharacteristics() {
    return IMMUTABLE_TYPE_CHARS;
  }

  private static final class Serializer implements TypeSerializer<Slice> {
    @Override
    public Slice serialize(Slice slice) {
      return slice;
    }

    @Override
    public Slice deserialize(Slice slice) {
      return slice;
    }
  }
}
