// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.pool;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Simple element pool.
 *
 * @param <ElementT> type of elements being pooled.
 */
@NotThreadSafe
public final class SimplePool<ElementT> {
  private final ArrayDeque<ElementT> elements = new ArrayDeque<>();
  private final Supplier<ElementT> supplier;
  private final int maxCapacity;

  public SimplePool(Supplier<ElementT> supplier, int maxCapacity) {
    this.supplier = Objects.requireNonNull(supplier);
    this.maxCapacity = maxCapacity;
  }

  public ElementT get() {
    ElementT element = elements.pollFirst();
    if (element != null) {
      return element;
    }
    return supplier.get();
  }

  public void release(ElementT item) {
    if (elements.size() < maxCapacity) {
      elements.addFirst(item);
    }
  }
}
