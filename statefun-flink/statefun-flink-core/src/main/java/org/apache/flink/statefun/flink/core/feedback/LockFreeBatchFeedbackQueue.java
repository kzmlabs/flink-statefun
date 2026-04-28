// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.feedback;

import java.util.Deque;
import org.apache.flink.statefun.flink.core.queue.Locks;
import org.apache.flink.statefun.flink.core.queue.MpscQueue;

public final class LockFreeBatchFeedbackQueue<ElementT> implements FeedbackQueue<ElementT> {
  private static final int INITIAL_BUFFER_SIZE = 32 * 1024; // 32k

  private final MpscQueue<ElementT> queue = new MpscQueue<>(INITIAL_BUFFER_SIZE, Locks.spinLock());

  @Override
  public boolean addAndCheckIfWasEmpty(ElementT element) {
    final int size = queue.add(element);
    return size == 1;
  }

  @Override
  public Deque<ElementT> drainAll() {
    return queue.drainAll();
  }
}
