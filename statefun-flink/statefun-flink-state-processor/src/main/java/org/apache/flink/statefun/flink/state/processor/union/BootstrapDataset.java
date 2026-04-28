// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.state.processor.union;

import java.util.Objects;
import org.apache.flink.statefun.flink.state.processor.BootstrapDataRouterProvider;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Represents a single registered bootstrap dataset, containing pre-tagged/routed bootstrap data
 * entries.
 */
public class BootstrapDataset<T> {

  private final DataStream<T> dataSet;
  private final BootstrapDataRouterProvider<T> routerProvider;

  public BootstrapDataset(DataStream<T> dataSet, BootstrapDataRouterProvider<T> routerProvider) {
    this.dataSet = Objects.requireNonNull(dataSet);
    this.routerProvider = Objects.requireNonNull(routerProvider);
  }

  DataStream<T> getDataSet() {
    return dataSet;
  }

  BootstrapDataRouterProvider<T> getRouterProvider() {
    return routerProvider;
  }
}
