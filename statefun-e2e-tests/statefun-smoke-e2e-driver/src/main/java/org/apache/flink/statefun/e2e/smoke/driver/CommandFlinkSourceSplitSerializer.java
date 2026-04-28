// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.e2e.smoke.driver;

import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

public class CommandFlinkSourceSplitSerializer
    implements SimpleVersionedSerializer<CommandFlinkSourceSplit> {

  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public byte[] serialize(CommandFlinkSourceSplit split) throws IOException {
    return InstantiationUtil.serializeObject(split);
  }

  @Override
  public CommandFlinkSourceSplit deserialize(int version, byte[] serialized) throws IOException {
    try {
      return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to deserialize the split.", e);
    }
  }
}
