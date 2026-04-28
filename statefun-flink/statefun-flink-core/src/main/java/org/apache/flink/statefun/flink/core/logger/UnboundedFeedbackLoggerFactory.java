// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.logger;

import java.util.Objects;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;

public final class UnboundedFeedbackLoggerFactory<T> {
  private final Supplier<KeyGroupStream<T>> supplier;
  private final ToIntFunction<T> keyGroupAssigner;
  private final CheckpointedStreamOperations checkpointedStreamOperations;
  private final TypeSerializer<T> serializer;

  @Inject
  public UnboundedFeedbackLoggerFactory(
      @Label("key-group-supplier") Supplier<KeyGroupStream<T>> supplier,
      @Label("key-group-assigner") ToIntFunction<T> keyGroupAssigner,
      @Label("checkpoint-stream-ops") CheckpointedStreamOperations ops,
      @Label("envelope-serializer") TypeSerializer<T> serializer) {
    this.supplier = Objects.requireNonNull(supplier);
    this.keyGroupAssigner = Objects.requireNonNull(keyGroupAssigner);
    this.serializer = Objects.requireNonNull(serializer);
    this.checkpointedStreamOperations = Objects.requireNonNull(ops);
  }

  public UnboundedFeedbackLogger<T> create() {
    return new UnboundedFeedbackLogger<>(
        supplier, keyGroupAssigner, checkpointedStreamOperations, serializer);
  }
}
