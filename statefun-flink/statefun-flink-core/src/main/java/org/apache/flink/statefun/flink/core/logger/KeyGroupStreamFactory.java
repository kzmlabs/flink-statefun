// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.logger;

import java.util.function.Supplier;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;

public final class KeyGroupStreamFactory<T> implements Supplier<KeyGroupStream<T>> {
  private final IOManager ioManager;
  private final MemorySegmentPool memorySegmentPool;
  private final TypeSerializer<T> serializer;

  @Inject
  KeyGroupStreamFactory(
      @Label("io-manager") IOManager ioManager,
      @Label("in-memory-max-buffer-size") long inMemoryBufferSize,
      @Label("envelope-serializer") TypeSerializer<T> serializer) {
    this.ioManager = ioManager;
    this.serializer = serializer;
    this.memorySegmentPool = new MemorySegmentPool(inMemoryBufferSize);
  }

  @Override
  public KeyGroupStream<T> get() {
    return new KeyGroupStream<>(serializer, ioManager, memorySegmentPool);
  }
}
