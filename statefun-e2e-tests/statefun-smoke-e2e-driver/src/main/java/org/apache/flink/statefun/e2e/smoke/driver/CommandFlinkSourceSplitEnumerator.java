// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.e2e.smoke.driver;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SplitEnumerator;

public class CommandFlinkSourceSplitEnumerator
    implements SplitEnumerator<CommandFlinkSourceSplit, HashSet<CommandFlinkSourceSplit>> {
  private final List<CommandFlinkSourceSplit> splits;

  public CommandFlinkSourceSplitEnumerator() {
    this.splits = new ArrayList<>();
  }

  @Override
  public void start() {}

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

  @Override
  public void addSplitsBack(List<CommandFlinkSourceSplit> splits, int subTaskId) {
    this.splits.addAll(splits);
  }

  @Override
  public void addReader(int subtaskId) {
    this.splits.add(new CommandFlinkSourceSplit(subtaskId));
  }

  @Override
  public HashSet<CommandFlinkSourceSplit> snapshotState(long l) {
    return new HashSet<>(this.splits);
  }

  @Override
  public void close() {}
}
