// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.pool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SimplePoolTest {

  @Test
  void getReturnsSupplierOutputWhenPoolIsEmpty() {
    AtomicInteger seq = new AtomicInteger();
    SimplePool<Integer> pool = new SimplePool<>(seq::incrementAndGet, 3);

    assertThat(pool.get()).isEqualTo(1);
    assertThat(pool.get()).isEqualTo(2);
    assertThat(pool.get()).isEqualTo(3);
  }

  @Test
  void releasedItemsAreReturnedBeforeSupplierIsConsultedAgain() {
    AtomicInteger seq = new AtomicInteger();
    SimplePool<Integer> pool = new SimplePool<>(seq::incrementAndGet, 5);

    Integer first = pool.get();
    Integer second = pool.get();
    pool.release(first);
    pool.release(second);

    // LIFO: release order is first then second, so the next get() returns the most-recently
    // released item. This matters for cache locality of pooled buffers.
    assertThat(pool.get()).isEqualTo(second);
    assertThat(pool.get()).isEqualTo(first);
    // Pool now empty -> falls back to supplier.
    assertThat(pool.get()).isEqualTo(3);
  }

  @Test
  void releaseDropsItemsOverMaxCapacity() {
    AtomicInteger seq = new AtomicInteger();
    SimplePool<Integer> pool = new SimplePool<>(seq::incrementAndGet, 2);

    pool.release(100);
    pool.release(101);
    pool.release(102); // exceeds capacity, should be dropped

    // Take 2 items — must be 101 and 100 (LIFO of what stayed in the pool).
    assertThat(pool.get()).isEqualTo(101);
    assertThat(pool.get()).isEqualTo(100);
    // Pool now empty -> supplier produces 1 (sequence starts fresh, never used yet).
    assertThat(pool.get()).isEqualTo(1);
  }

  @Test
  void zeroCapacityPoolAlwaysCallsSupplier() {
    AtomicInteger sequence = new AtomicInteger();
    SimplePool<Integer> pool = new SimplePool<>(sequence::incrementAndGet, 0);

    pool.release(99); // dropped, capacity 0
    assertThat(pool.get()).isEqualTo(1);
    pool.release(99); // still dropped
    assertThat(pool.get()).isEqualTo(2);
  }

  @Test
  void rejectsNullSupplier() {
    assertThatThrownBy(() -> new SimplePool<Object>(null, 5))
        .isInstanceOf(NullPointerException.class);
  }
}
