// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.metrics;

import java.util.Objects;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

public class FlinkFunctionDispatcherMetrics implements FunctionDispatcherMetrics {
  private final Counter inflightAsyncOperations;

  public FlinkFunctionDispatcherMetrics(MetricGroup operatorGroup) {
    Objects.requireNonNull(operatorGroup, "operatorGroup");

    this.inflightAsyncOperations = operatorGroup.counter("inflightAsyncOps");
  }

  @Override
  public void asyncOperationRegistered() {
    inflightAsyncOperations.inc();
  }

  @Override
  public void asyncOperationCompleted() {
    inflightAsyncOperations.dec();
  }
}
