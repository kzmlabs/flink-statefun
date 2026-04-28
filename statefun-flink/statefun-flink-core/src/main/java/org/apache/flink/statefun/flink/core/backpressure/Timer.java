// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.backpressure;

interface Timer {

  long now();

  void sleep(long durationNanos);
}
