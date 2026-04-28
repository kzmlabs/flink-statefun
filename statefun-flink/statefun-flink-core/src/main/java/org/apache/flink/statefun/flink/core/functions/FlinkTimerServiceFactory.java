// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.functions;

import java.util.Objects;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;

final class FlinkTimerServiceFactory implements TimerServiceFactory {

  private static final String DELAYED_MSG_TIMER_SERVICE_NAME = "delayed-messages-timer-service";

  private final InternalTimeServiceManager<String> timeServiceManager;

  @SuppressWarnings("unchecked")
  FlinkTimerServiceFactory(InternalTimeServiceManager<?> timeServiceManager) {
    this.timeServiceManager =
        (InternalTimeServiceManager<String>) Objects.requireNonNull(timeServiceManager);
  }

  @Override
  public InternalTimerService<VoidNamespace> createTimerService(
      Triggerable<String, VoidNamespace> triggerable) {

    return timeServiceManager.getInternalTimerService(
        DELAYED_MSG_TIMER_SERVICE_NAME,
        StringSerializer.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        triggerable);
  }
}
