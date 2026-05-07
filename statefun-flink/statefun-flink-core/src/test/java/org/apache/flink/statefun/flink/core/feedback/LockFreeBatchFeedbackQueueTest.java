// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.feedback;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Deque;
import org.junit.jupiter.api.Test;

class LockFreeBatchFeedbackQueueTest {

  @Test
  void firstAddReportsQueueWasEmpty() {
    LockFreeBatchFeedbackQueue<String> queue = new LockFreeBatchFeedbackQueue<>();

    assertThat(queue.addAndCheckIfWasEmpty("first")).isTrue();
  }

  @Test
  void subsequentAddsReportQueueWasNotEmpty() {
    LockFreeBatchFeedbackQueue<String> queue = new LockFreeBatchFeedbackQueue<>();
    queue.addAndCheckIfWasEmpty("first");

    assertThat(queue.addAndCheckIfWasEmpty("second")).isFalse();
    assertThat(queue.addAndCheckIfWasEmpty("third")).isFalse();
  }

  @Test
  void drainAllReturnsAddedElementsInOrder() {
    LockFreeBatchFeedbackQueue<String> queue = new LockFreeBatchFeedbackQueue<>();
    queue.addAndCheckIfWasEmpty("a");
    queue.addAndCheckIfWasEmpty("b");
    queue.addAndCheckIfWasEmpty("c");

    Deque<String> drained = queue.drainAll();

    assertThat(drained).containsExactly("a", "b", "c");
  }

  @Test
  void drainAllOnEmptyQueueReturnsEmpty() {
    LockFreeBatchFeedbackQueue<String> queue = new LockFreeBatchFeedbackQueue<>();

    Deque<String> drained = queue.drainAll();

    assertThat(drained).isEmpty();
  }

  @Test
  void drainAllResetsQueueSoNextAddReportsEmpty() {
    LockFreeBatchFeedbackQueue<String> queue = new LockFreeBatchFeedbackQueue<>();
    queue.addAndCheckIfWasEmpty("a");
    queue.addAndCheckIfWasEmpty("b");
    queue.drainAll();

    // After drainAll the queue should be empty — the next add should report wasEmpty=true.
    assertThat(queue.addAndCheckIfWasEmpty("c")).isTrue();
  }
}
