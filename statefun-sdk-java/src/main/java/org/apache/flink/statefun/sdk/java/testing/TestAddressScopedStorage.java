// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.java.testing;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.ValueSpec;

final class TestAddressScopedStorage implements AddressScopedStorage {

  private final ConcurrentHashMap<String, Object> storage = new ConcurrentHashMap<>();

  @Override
  public <T> Optional<T> get(ValueSpec<T> spec) {
    Objects.requireNonNull(spec);
    Object value = storage.get(spec.name());

    @SuppressWarnings("unchecked")
    Optional<T> maybeValue = (Optional<T>) Optional.ofNullable(value);
    return maybeValue;
  }

  @Override
  public <T> void set(ValueSpec<T> spec, T value) {
    Objects.requireNonNull(spec);
    Objects.requireNonNull(value);
    storage.put(spec.name(), value);
  }

  @Override
  public <T> void remove(ValueSpec<T> spec) {
    Objects.requireNonNull(spec);
    storage.remove(spec.name());
  }
}
