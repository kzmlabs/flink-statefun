// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.e2e.smoke.driver;

import java.util.HashSet;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.statefun.e2e.smoke.SmokeRunnerParameters;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

final class CommandFlinkSource
    implements Source<TypedValue, CommandFlinkSourceSplit, HashSet<CommandFlinkSourceSplit>> {
  private static final long serialVersionUID = 1;
  private final SmokeRunnerParameters parameters;

  CommandFlinkSource(SmokeRunnerParameters parameters) {
    this.parameters = parameters;
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SplitEnumerator<CommandFlinkSourceSplit, HashSet<CommandFlinkSourceSplit>>
      createEnumerator(SplitEnumeratorContext<CommandFlinkSourceSplit> splitEnumeratorContext)
          throws Exception {
    return new CommandFlinkSourceSplitEnumerator();
  }

  @Override
  public SplitEnumerator<CommandFlinkSourceSplit, HashSet<CommandFlinkSourceSplit>>
      restoreEnumerator(
          SplitEnumeratorContext<CommandFlinkSourceSplit> splitEnumeratorContext,
          HashSet<CommandFlinkSourceSplit> enumChck)
          throws Exception {
    return new CommandFlinkSourceSplitEnumerator();
  }

  @Override
  public SimpleVersionedSerializer<CommandFlinkSourceSplit> getSplitSerializer() {
    return new CommandFlinkSourceSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<HashSet<CommandFlinkSourceSplit>>
      getEnumeratorCheckpointSerializer() {
    return new CommandFlinkSourceSplitCheckpointSerializer();
  }

  @Override
  public SourceReader<TypedValue, CommandFlinkSourceSplit> createReader(
      SourceReaderContext sourceReaderContext) throws Exception {
    return new CommandFlinkSourceReader(this.parameters);
  }
}
