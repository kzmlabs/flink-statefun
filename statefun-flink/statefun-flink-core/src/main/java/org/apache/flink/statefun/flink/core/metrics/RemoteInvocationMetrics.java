// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.metrics;

public interface RemoteInvocationMetrics {

  void remoteInvocationFailures();

  void remoteInvocationLatency(long elapsed);
}
