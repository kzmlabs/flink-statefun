/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
