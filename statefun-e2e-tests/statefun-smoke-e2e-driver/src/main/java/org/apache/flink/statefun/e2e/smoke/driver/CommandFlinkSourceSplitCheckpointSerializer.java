// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.e2e.smoke.driver;

import java.io.IOException;
import java.util.HashSet;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

public class CommandFlinkSourceSplitCheckpointSerializer
    implements SimpleVersionedSerializer<HashSet<CommandFlinkSourceSplit>> {
  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public byte[] serialize(HashSet<CommandFlinkSourceSplit> commandFlinkSourceSplits)
      throws IOException {
    return InstantiationUtil.serializeObject(commandFlinkSourceSplits);
  }

  @Override
  public HashSet<CommandFlinkSourceSplit> deserialize(int version, byte[] serialized)
      throws IOException {
    try {
      return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to deserialize the checkpointed splits.", e);
    }
  }
}
