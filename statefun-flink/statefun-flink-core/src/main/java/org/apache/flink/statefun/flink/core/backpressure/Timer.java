// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.backpressure;

interface Timer {

  long now();

  void sleep(long durationNanos);
}
