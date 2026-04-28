// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.metrics;

public interface RemoteInvocationMetrics {

  void remoteInvocationFailures();

  void remoteInvocationLatency(long elapsed);
}
