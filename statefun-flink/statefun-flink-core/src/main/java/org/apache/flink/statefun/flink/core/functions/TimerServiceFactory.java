// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.functions;

import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;

interface TimerServiceFactory {
  InternalTimerService<VoidNamespace> createTimerService(
      Triggerable<String, VoidNamespace> triggerable);
}
