// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.state;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.statefun.sdk.state.AppendingBufferAccessor;

final class FlinkAppendingBufferAccessor<E> implements AppendingBufferAccessor<E> {

  private final ListState<E> handle;

  FlinkAppendingBufferAccessor(ListState<E> handle) {
    this.handle = Objects.requireNonNull(handle);
  }

  @Override
  public void append(@Nonnull E element) {
    try {
      this.handle.add(element);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void appendAll(@Nonnull List<E> elements) {
    try {
      this.handle.addAll(elements);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void replaceWith(@Nonnull List<E> elements) {
    try {
      this.handle.update(elements);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  @Override
  public Iterable<E> view() {
    try {
      return this.handle.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    try {
      this.handle.clear();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
