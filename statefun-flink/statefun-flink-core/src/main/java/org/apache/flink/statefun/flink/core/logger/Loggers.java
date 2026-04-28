// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.statefun.flink.core.di.ObjectContainer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard.Lease;

public final class Loggers {
  private Loggers() {}

  public static UnboundedFeedbackLoggerFactory<?> unboundedSpillableLoggerFactory(
      IOManager ioManager,
      int maxParallelism,
      long inMemoryMaxBufferSize,
      TypeSerializer<?> serializer,
      Function<?, ?> keySelector) {

    ObjectContainer container =
        unboundedSpillableLoggerContainer(
            ioManager, maxParallelism, inMemoryMaxBufferSize, serializer, keySelector);

    return container.get(UnboundedFeedbackLoggerFactory.class);
  }

  /** Wires the required dependencies to construct an {@link UnboundedFeedbackLogger}. */
  @VisibleForTesting
  static ObjectContainer unboundedSpillableLoggerContainer(
      IOManager ioManager,
      int maxParallelism,
      long inMemoryMaxBufferSize,
      TypeSerializer<?> serializer,
      Function<?, ?> keySelector) {

    ObjectContainer container = new ObjectContainer();
    container.add("max-parallelism", int.class, maxParallelism);
    container.add("in-memory-max-buffer-size", long.class, inMemoryMaxBufferSize);
    container.add("io-manager", IOManager.class, ioManager);
    container.add("key-group-supplier", Supplier.class, KeyGroupStreamFactory.class);
    container.add(
        "key-group-assigner", ToIntFunction.class, new KeyAssigner<>(keySelector, maxParallelism));
    container.add("envelope-serializer", TypeSerializer.class, serializer);
    container.add(
        "checkpoint-stream-ops",
        CheckpointedStreamOperations.class,
        KeyedStateCheckpointOutputStreamOps.INSTANCE);
    container.add(UnboundedFeedbackLoggerFactory.class);
    return container;
  }

  private enum KeyedStateCheckpointOutputStreamOps implements CheckpointedStreamOperations {
    INSTANCE;

    @Override
    public void requireKeyedStateCheckpointed(OutputStream stream) {
      if (stream instanceof KeyedStateCheckpointOutputStream) {
        return;
      }
      throw new IllegalStateException("Not a KeyedStateCheckpointOutputStream");
    }

    @Override
    public Iterable<Integer> keyGroupList(OutputStream stream) {
      return cast(stream).getKeyGroupList();
    }

    @Override
    public void startNewKeyGroup(OutputStream stream, int keyGroup) throws IOException {
      cast(stream).startNewKeyGroup(keyGroup);
    }

    @Override
    @SuppressWarnings("resource")
    public Closeable acquireLease(OutputStream stream) {
      Preconditions.checkState(stream instanceof KeyedStateCheckpointOutputStream);
      try {
        Lease lease = cast(stream).acquireLease();
        return lease::close;
      } catch (IOException e) {
        throw new IllegalStateException("Unable to obtain a lease for the input stream.", e);
      }
    }

    private static KeyedStateCheckpointOutputStream cast(OutputStream stream) {
      Preconditions.checkState(stream instanceof KeyedStateCheckpointOutputStream);
      return (KeyedStateCheckpointOutputStream) stream;
    }
  }

  private static final class KeyAssigner<T> implements ToIntFunction<T> {
    private final Function<T, ?> keySelector;
    private final int maxParallelism;

    private KeyAssigner(Function<T, ?> keySelector, int maxParallelism) {
      this.keySelector = Objects.requireNonNull(keySelector);
      this.maxParallelism = maxParallelism;
    }

    @Override
    public int applyAsInt(T value) {
      Object key = keySelector.apply(value);
      return KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
    }
  }
}
