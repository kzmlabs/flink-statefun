// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.metrics;

public interface FunctionDispatcherMetrics {

  void asyncOperationRegistered();

  void asyncOperationCompleted();
}
