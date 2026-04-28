// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.metrics;

import org.apache.flink.statefun.sdk.metrics.Counter;

final class FlinkMetricUtil {

  private FlinkMetricUtil() {}

  static Counter wrapFlinkCounterAsSdkCounter(org.apache.flink.metrics.Counter internalCounter) {
    return new Counter() {
      @Override
      public void inc(long amount) {
        internalCounter.inc(amount);
      }

      @Override
      public void dec(long amount) {
        internalCounter.dec(amount);
      }
    };
  }
}
