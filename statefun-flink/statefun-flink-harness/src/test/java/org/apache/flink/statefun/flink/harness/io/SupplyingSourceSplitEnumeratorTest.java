// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.harness.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

class SupplyingSourceSplitEnumeratorTest {

  @Test
  void addReaderProvisionsSplitForSubtask() {
    SupplyingSourceSplitEnumerator<String> e = new SupplyingSourceSplitEnumerator<>();

    e.addReader(0);
    e.addReader(1);

    HashSet<SupplyingSourceSplit<String>> snapshot = e.snapshotState(0);
    assertThat(snapshot).hasSize(2);
    assertThat(snapshot)
        .extracting(SupplyingSourceSplit::splitId)
        .containsExactlyInAnyOrder("0", "1");
  }

  @Test
  void addSplitsBackKeepsExistingSplitsAndAddsNew() {
    SupplyingSourceSplitEnumerator<String> e = new SupplyingSourceSplitEnumerator<>();
    e.addReader(0);

    SupplyingSourceSplit<String> recovered1 = new SupplyingSourceSplit<>(10);
    SupplyingSourceSplit<String> recovered2 = new SupplyingSourceSplit<>(11);
    e.addSplitsBack(Arrays.asList(recovered1, recovered2), 0);

    HashSet<SupplyingSourceSplit<String>> snapshot = e.snapshotState(0);
    assertThat(snapshot).hasSize(3);
    assertThat(snapshot)
        .extracting(SupplyingSourceSplit::splitId)
        .containsExactlyInAnyOrder("0", "10", "11");
  }

  @Test
  void startAndCloseAndHandleSplitRequestAreNoOps() {
    SupplyingSourceSplitEnumerator<String> e = new SupplyingSourceSplitEnumerator<>();

    e.start();
    e.handleSplitRequest(0, "host");
    e.close();

    // The contract is that none of these alter state — adding a reader after still works.
    e.addReader(99);
    assertThat(e.snapshotState(1)).hasSize(1);
  }
}
