// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.harness.io;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SplitEnumerator;

public class SupplyingSourceSplitEnumerator<T>
    implements SplitEnumerator<SupplyingSourceSplit<T>, HashSet<SupplyingSourceSplit<T>>> {
  private final List<SupplyingSourceSplit<T>> splits;

  public SupplyingSourceSplitEnumerator() {
    this.splits = new ArrayList<>();
  }

  @Override
  public void start() {}

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

  @Override
  public void addSplitsBack(List<SupplyingSourceSplit<T>> splits, int subTaskId) {
    this.splits.addAll(splits);
  }

  @Override
  public void addReader(int subtaskId) {
    this.splits.add(new SupplyingSourceSplit<T>(subtaskId));
  }

  @Override
  public HashSet<SupplyingSourceSplit<T>> snapshotState(long l) {
    return new HashSet<>(this.splits);
  }

  @Override
  public void close() {}
}
