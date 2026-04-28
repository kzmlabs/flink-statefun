// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.harness.io;

import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

public class SupplyingSourceSplitSerializer<T>
    implements SimpleVersionedSerializer<SupplyingSourceSplit<T>> {

  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public byte[] serialize(SupplyingSourceSplit<T> split) throws IOException {
    return InstantiationUtil.serializeObject(split);
  }

  @Override
  public SupplyingSourceSplit<T> deserialize(int version, byte[] serialized) throws IOException {
    try {
      return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to deserialize the split.", e);
    }
  }
}
