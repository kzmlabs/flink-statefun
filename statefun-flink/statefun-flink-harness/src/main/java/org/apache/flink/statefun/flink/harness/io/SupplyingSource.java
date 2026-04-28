// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.harness.io;

import java.util.HashSet;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class SupplyingSource<T>
    implements Source<T, SupplyingSourceSplit<T>, HashSet<SupplyingSourceSplit<T>>> {
  private static final long serialVersionUID = 1;

  private final SerializableSupplier<T> supplier;

  SupplyingSource(SerializableSupplier<T> supplier) {
    this.supplier = supplier;
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SplitEnumerator<SupplyingSourceSplit<T>, HashSet<SupplyingSourceSplit<T>>>
      createEnumerator(SplitEnumeratorContext<SupplyingSourceSplit<T>> splitEnumeratorContext)
          throws Exception {
    return new SupplyingSourceSplitEnumerator<>();
  }

  @Override
  public SplitEnumerator<SupplyingSourceSplit<T>, HashSet<SupplyingSourceSplit<T>>>
      restoreEnumerator(
          SplitEnumeratorContext<SupplyingSourceSplit<T>> splitEnumeratorContext,
          HashSet<SupplyingSourceSplit<T>> enumChck)
          throws Exception {
    return new SupplyingSourceSplitEnumerator<>();
  }

  @Override
  public SimpleVersionedSerializer<SupplyingSourceSplit<T>> getSplitSerializer() {
    return new SupplyingSourceSplitSerializer<>();
  }

  @Override
  public SimpleVersionedSerializer<HashSet<SupplyingSourceSplit<T>>>
      getEnumeratorCheckpointSerializer() {
    return null;
  }

  @Override
  public SourceReader<T, SupplyingSourceSplit<T>> createReader(
      SourceReaderContext sourceReaderContext) {
    return new SupplyingSourceReader<>(this.supplier);
  }
}
