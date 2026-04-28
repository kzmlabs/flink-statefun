// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public interface CheckpointedStreamOperations {

  void requireKeyedStateCheckpointed(OutputStream keyedStateCheckpointOutputStream);

  Iterable<Integer> keyGroupList(OutputStream stream);

  void startNewKeyGroup(OutputStream stream, int keyGroup) throws IOException;

  Closeable acquireLease(OutputStream keyedStateCheckpointOutputStream);
}
