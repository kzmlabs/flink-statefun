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
