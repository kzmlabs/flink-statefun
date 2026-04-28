// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.metrics;

public interface FunctionDispatcherMetrics {

  void asyncOperationRegistered();

  void asyncOperationCompleted();
}
