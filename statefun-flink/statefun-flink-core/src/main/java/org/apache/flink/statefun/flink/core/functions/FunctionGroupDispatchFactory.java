// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.functions;

import java.util.Map;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.util.OutputTag;

public final class FunctionGroupDispatchFactory
    implements OneInputStreamOperatorFactory<Message, Message> {

  private static final long serialVersionUID = 1;

  private final StatefulFunctionsConfig configuration;

  private final Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs;

  private transient MailboxExecutor mailboxExecutor;

  public FunctionGroupDispatchFactory(
      StatefulFunctionsConfig configuration,
      Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs) {
    this.configuration = configuration;
    this.sideOutputs = sideOutputs;
  }

  @Override
  public <T extends StreamOperator<Message>> T createStreamOperator(
      StreamOperatorParameters<Message> streamOperatorParameters) {
    FunctionGroupOperator fn =
        new FunctionGroupOperator(sideOutputs, configuration, streamOperatorParameters);

    return (T) fn;
  }

  @Override
  public void setChainingStrategy(ChainingStrategy chainingStrategy) {
    // We ignore the chaining strategy, because we only use ChainingStrategy.ALWAYS
  }

  @Override
  public ChainingStrategy getChainingStrategy() {
    return ChainingStrategy.ALWAYS;
  }

  @Override
  public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
    return FunctionGroupOperator.class;
  }
}
