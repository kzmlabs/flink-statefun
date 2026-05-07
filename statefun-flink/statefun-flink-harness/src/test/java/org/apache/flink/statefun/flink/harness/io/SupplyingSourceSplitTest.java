// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.harness.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SupplyingSourceSplitTest {

  @Test
  void splitIdMatchesConstructorIdAsString() {
    SupplyingSourceSplit<String> split = new SupplyingSourceSplit<>(7);

    assertThat(split.splitId()).isEqualTo("7");
  }

  @Test
  void getNextNonBlockingReturnsNullWhenQueueIsEmpty() throws Exception {
    SupplyingSourceSplit<String> split = new SupplyingSourceSplit<>(0);

    assertThat(split.getNext(false)).isNull();
  }

  @Test
  void addRecordEnqueuesForNonBlockingDrain() throws Exception {
    SupplyingSourceSplit<String> split = new SupplyingSourceSplit<>(0);

    split.addRecord("a").addRecord("b");

    assertThat(split.getNext(false)).isEqualTo("a");
    assertThat(split.getNext(false)).isEqualTo("b");
    assertThat(split.getNext(false)).isNull();
  }

  @Test
  @Timeout(2)
  void getNextBlockingWaitsUntilAddRecord() throws Exception {
    SupplyingSourceSplit<String> split = new SupplyingSourceSplit<>(0);
    AtomicReference<String> received = new AtomicReference<>();

    Thread consumer =
        new Thread(
            () -> {
              try {
                received.set(split.getNext(true));
              } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
              }
            });
    consumer.start();
    // Sleep a tick so the consumer is parked in take().
    Thread.sleep(50);
    split.addRecord("late");
    consumer.join(1500);

    assertThat(received.get()).isEqualTo("late");
  }
}
