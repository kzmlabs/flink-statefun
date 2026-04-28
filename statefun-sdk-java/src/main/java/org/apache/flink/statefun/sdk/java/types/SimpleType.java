// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.java.types;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;

/**
 * A utility to create simple {@link Type} implementations.
 *
 * @param <T> the Java type handled by this {@link Type}.
 */
public final class SimpleType<T> implements Type<T> {

  @FunctionalInterface
  public interface Fn<I, O> {
    O apply(I input) throws Throwable;
  }

  public static <T> Type<T> simpleTypeFrom(
      TypeName typeName, Fn<T, byte[]> serialize, Fn<byte[], T> deserialize) {
    return new SimpleType<>(typeName, serialize, deserialize, Collections.emptySet());
  }

  public static <T> Type<T> simpleImmutableTypeFrom(
      TypeName typeName, Fn<T, byte[]> serialize, Fn<byte[], T> deserialize) {
    return new SimpleType<>(
        typeName, serialize, deserialize, EnumSet.of(TypeCharacteristics.IMMUTABLE_VALUES));
  }

  private final TypeName typeName;
  private final TypeSerializer<T> serializer;
  private final Set<TypeCharacteristics> typeCharacteristics;

  private SimpleType(
      TypeName typeName,
      Fn<T, byte[]> serialize,
      Fn<byte[], T> deserialize,
      Set<TypeCharacteristics> typeCharacteristics) {
    this.typeName = Objects.requireNonNull(typeName);
    this.serializer = new Serializer<>(serialize, deserialize);
    this.typeCharacteristics = Collections.unmodifiableSet(typeCharacteristics);
  }

  @Override
  public TypeName typeName() {
    return typeName;
  }

  @Override
  public TypeSerializer<T> typeSerializer() {
    return serializer;
  }

  @Override
  public Set<TypeCharacteristics> typeCharacteristics() {
    return typeCharacteristics;
  }

  private static final class Serializer<T> implements TypeSerializer<T> {
    private final Fn<T, byte[]> serialize;
    private final Fn<byte[], T> deserialize;

    private Serializer(Fn<T, byte[]> serialize, Fn<byte[], T> deserialize) {
      this.serialize = Objects.requireNonNull(serialize);
      this.deserialize = Objects.requireNonNull(deserialize);
    }

    @Override
    public Slice serialize(T value) {
      try {
        byte[] bytes = serialize.apply(value);
        return Slices.wrap(bytes);
      } catch (Throwable throwable) {
        throw new IllegalStateException(throwable);
      }
    }

    @Override
    public T deserialize(Slice input) {
      try {
        byte[] bytes = input.toByteArray();
        return deserialize.apply(bytes);
      } catch (Throwable throwable) {
        throw new IllegalStateException(throwable);
      }
    }
  }
}
