// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.sdk.state;

import java.util.Objects;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;

@ForRuntime
public final class RemotePersistedValue {
  private final String name;
  private final TypeName type;
  private final Expiration expiration;
  private Accessor<byte[]> accessor;

  private RemotePersistedValue(
      String name, TypeName type, Expiration expiration, Accessor<byte[]> accessor) {
    this.name = Objects.requireNonNull(name);
    this.type = Objects.requireNonNull(type);
    this.expiration = Objects.requireNonNull(expiration);
    this.accessor = Objects.requireNonNull(accessor);
  }

  public static RemotePersistedValue of(String stateName, TypeName typeName) {
    return new RemotePersistedValue(
        stateName, typeName, Expiration.none(), new NonFaultTolerantAccessor<>());
  }

  public static RemotePersistedValue of(
      String stateName, TypeName typeName, Expiration expiration) {
    return new RemotePersistedValue(
        stateName, typeName, expiration, new NonFaultTolerantAccessor<>());
  }

  public byte[] get() {
    return accessor.get();
  }

  public void set(byte[] value) {
    accessor.set(value);
  }

  public void clear() {
    accessor.clear();
  }

  public String name() {
    return name;
  }

  public TypeName type() {
    return type;
  }

  public Expiration expiration() {
    return expiration;
  }

  @ForRuntime
  void setAccessor(Accessor<byte[]> newAccessor) {
    this.accessor = Objects.requireNonNull(newAccessor);
  }

  private static final class NonFaultTolerantAccessor<E> implements Accessor<E> {
    private E element;

    @Override
    public void set(E element) {
      this.element = element;
    }

    @Override
    public E get() {
      return element;
    }

    @Override
    public void clear() {
      element = null;
    }
  }
}
