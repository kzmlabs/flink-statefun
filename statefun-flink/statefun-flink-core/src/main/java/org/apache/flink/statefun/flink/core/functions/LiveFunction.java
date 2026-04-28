// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.functions;

import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.metrics.FunctionTypeMetrics;
import org.apache.flink.statefun.sdk.Context;

interface LiveFunction {

  void receive(Context context, Message message);

  FunctionTypeMetrics metrics();
}
