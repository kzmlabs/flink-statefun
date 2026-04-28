// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.logger;

import java.util.ArrayDeque;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentSource;

@NotThreadSafe
final class MemorySegmentPool implements MemorySegmentSource {
  static final int PAGE_SIZE = 64 * 1024;

  private final ArrayDeque<MemorySegment> pool;
  private final long inMemoryBufferSize;
  private long totalAllocatedMemory;

  MemorySegmentPool(long inMemoryBufferSize) {
    this.pool = new ArrayDeque<>();
    this.inMemoryBufferSize = inMemoryBufferSize;
  }

  @Nullable
  @Override
  public MemorySegment nextSegment() {
    MemorySegment segment = pool.pollFirst();
    if (segment != null) {
      return segment;
    }
    //
    // no segments in the pool, try to allocate one.
    //
    if (!hasRemainingCapacity()) {
      return null;
    }
    segment = MemorySegmentFactory.allocateUnpooledSegment(PAGE_SIZE);
    totalAllocatedMemory += PAGE_SIZE;
    return segment;
  }

  void release(MemorySegment segment) {
    if (totalAllocatedMemory > inMemoryBufferSize) {
      //
      // we previously overdraft.
      //
      segment.free();
      totalAllocatedMemory -= PAGE_SIZE;
      return;
    }
    pool.add(segment);
  }

  int getSegmentSize() {
    return PAGE_SIZE;
  }

  void ensureAtLeastOneSegmentPresent() {
    if (!pool.isEmpty()) {
      //
      // the next allocation would succeeded because the pool is not empty
      //
      return;
    }
    if (hasRemainingCapacity()) {
      //
      // the next allocation would succeeded because the total allocated size is within the allowed
      // range
      //
      return;
    }
    //
    // we overdraft momentarily.
    //
    MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(PAGE_SIZE);
    totalAllocatedMemory += PAGE_SIZE;
    pool.add(segment);
  }

  private boolean hasRemainingCapacity() {
    return totalAllocatedMemory + PAGE_SIZE <= inMemoryBufferSize;
  }
}
