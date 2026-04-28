// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.state;

import java.io.IOException;
import java.util.Objects;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.statefun.sdk.state.Accessor;

final class FlinkValueAccessor<T> implements Accessor<T> {

  private final ValueState<T> handle;

  FlinkValueAccessor(ValueState<T> handle) {
    this.handle = Objects.requireNonNull(handle);
  }

  @Override
  public void set(T value) {
    try {
      if (value == null) {
        handle.clear();
      } else {
        handle.update(value);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public T get() {
    try {
      return handle.value();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    handle.clear();
  }
}
