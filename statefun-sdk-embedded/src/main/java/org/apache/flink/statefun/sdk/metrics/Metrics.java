// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.metrics;

public interface Metrics {

  /**
   * Retrieves (or creates) a counter metric with this name.
   *
   * @param name a metric name
   * @return a counter.
   */
  Counter counter(String name);
}
