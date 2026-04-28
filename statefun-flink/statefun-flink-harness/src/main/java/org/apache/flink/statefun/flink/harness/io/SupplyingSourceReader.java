// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.harness.io;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;

public class SupplyingSourceReader<T> implements SourceReader<T, SupplyingSourceSplit<T>> {
  private final SerializableSupplier<T> supplier;

  public SupplyingSourceReader(SerializableSupplier<T> supplier) {
    this.supplier = supplier;
  }

  @Override
  public void start() {}

  @Override
  public InputStatus pollNext(ReaderOutput<T> readerOutput) {
    T value = supplier.get();
    if (Objects.isNull(value)) {
      return InputStatus.END_OF_INPUT;
    } else {
      readerOutput.collect(value);
      return InputStatus.MORE_AVAILABLE;
    }
  }

  @Override
  public List<SupplyingSourceSplit<T>> snapshotState(long l) {
    return List.of();
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    return null;
  }

  @Override
  public void addSplits(List<SupplyingSourceSplit<T>> list) {}

  @Override
  public void notifyNoMoreSplits() {}

  @Override
  public void close() throws Exception {}
}
