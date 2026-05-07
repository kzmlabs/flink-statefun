// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.harness.io;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SupplyingSourceSplitSerializerTest {

  @Test
  void serializerVersionIsZero() {
    SupplyingSourceSplitSerializer<String> serializer = new SupplyingSourceSplitSerializer<>();

    assertThat(serializer.getVersion()).isZero();
  }

  @Test
  void roundtripsSplitWithItsId() throws Exception {
    SupplyingSourceSplitSerializer<String> serializer = new SupplyingSourceSplitSerializer<>();
    SupplyingSourceSplit<String> original = new SupplyingSourceSplit<>(42);

    byte[] bytes = serializer.serialize(original);
    SupplyingSourceSplit<String> restored = serializer.deserialize(0, bytes);

    assertThat(restored.splitId()).isEqualTo("42");
  }

  @Test
  void roundtripsSplitWithEnqueuedRecords() throws Exception {
    SupplyingSourceSplitSerializer<String> serializer = new SupplyingSourceSplitSerializer<>();
    SupplyingSourceSplit<String> original = new SupplyingSourceSplit<>(0);
    original.addRecord("a").addRecord("b").addRecord("c");

    byte[] bytes = serializer.serialize(original);
    SupplyingSourceSplit<String> restored = serializer.deserialize(0, bytes);

    // The records queue is a LinkedBlockingQueue<>, which serializes its current contents.
    // After restore the records should still drain in original order.
    assertThat(restored.getNext(false)).isEqualTo("a");
    assertThat(restored.getNext(false)).isEqualTo("b");
    assertThat(restored.getNext(false)).isEqualTo("c");
  }
}
