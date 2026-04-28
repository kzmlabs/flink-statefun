// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.functions;

import java.util.Objects;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.flink.core.metrics.FunctionTypeMetrics;
import org.apache.flink.statefun.sdk.Context;

final class StatefulFunction implements LiveFunction {
  private final org.apache.flink.statefun.sdk.StatefulFunction statefulFunction;
  private final FunctionTypeMetrics metrics;
  private final MessageFactory messageFactory;

  StatefulFunction(
      org.apache.flink.statefun.sdk.StatefulFunction statefulFunction,
      FunctionTypeMetrics metrics,
      MessageFactory messageFactory) {

    this.statefulFunction = Objects.requireNonNull(statefulFunction);
    this.metrics = Objects.requireNonNull(metrics);
    this.messageFactory = Objects.requireNonNull(messageFactory);
  }

  @Override
  public void receive(Context context, Message message) {
    final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      ClassLoader targetClassLoader = statefulFunction.getClass().getClassLoader();
      Thread.currentThread().setContextClassLoader(targetClassLoader);
      Object payload = message.payload(messageFactory, targetClassLoader);
      statefulFunction.invoke(context, payload);
    } catch (Exception e) {
      throw new StatefulFunctionInvocationException(context.self().type(), e);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  @Override
  public FunctionTypeMetrics metrics() {
    return metrics;
  }
}
