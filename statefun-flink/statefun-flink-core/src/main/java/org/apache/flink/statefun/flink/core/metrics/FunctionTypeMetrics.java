// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.metrics;

import org.apache.flink.statefun.sdk.metrics.Metrics;

public interface FunctionTypeMetrics extends RemoteInvocationMetrics {

  void incomingMessage();

  void outgoingLocalMessage();

  void outgoingRemoteMessage();

  void outgoingEgressMessage();

  void blockedAddress();

  void unblockedAddress();

  void asyncOperationRegistered();

  void asyncOperationCompleted();

  void appendBacklogMessages(int count);

  void consumeBacklogMessages(int count);

  Metrics functionTypeScopedMetrics();
}
