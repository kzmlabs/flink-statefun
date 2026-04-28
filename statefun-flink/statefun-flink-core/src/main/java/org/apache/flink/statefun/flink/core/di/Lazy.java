// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.di;

import java.util.Objects;
import javax.annotation.Nullable;

@SuppressWarnings({"unchecked", "unused", "WeakerAccess"})
public final class Lazy<T> {
  private final Class<T> type;
  private final String label;
  private ObjectContainer container;

  @Nullable private T instance;

  public Lazy(Class<T> type) {
    this(type, null);
  }

  public Lazy(Class<T> type, String label) {
    this.type = type;
    this.label = label;
  }

  public Lazy(T instance) {
    this((Class<T>) instance.getClass(), null);
    this.instance = instance;
  }

  Lazy<T> withContainer(ObjectContainer container) {
    this.container = Objects.requireNonNull(container);
    return this;
  }

  public T get() {
    @Nullable T instance = this.instance;
    if (instance == null) {
      instance = container.get(type, label);
      this.instance = instance;
    }
    return instance;
  }
}
