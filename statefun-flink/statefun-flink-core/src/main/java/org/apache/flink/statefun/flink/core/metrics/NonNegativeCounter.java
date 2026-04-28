// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.metrics;

import org.apache.flink.metrics.Counter;

/**
 * A simple counter that can never go below zero.
 *
 * <p>This class is used in a non-thread safe manner, so it is important all modifications are
 * checked before updating the count. Otherwise, negative values might be reported.
 */
public class NonNegativeCounter implements Counter {

  private long count;

  @Override
  public void inc() {
    inc(1);
  }

  @Override
  public void inc(long value) {
    count += value;
  }

  @Override
  public void dec() {
    dec(1);
  }

  @Override
  public void dec(long value) {
    if (value > count) {
      count = 0;
    } else {
      count -= value;
    }
  }

  @Override
  public long getCount() {
    return count;
  }
}
