// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.state.processor.operator;

import org.apache.flink.core.fs.Path;
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

public class FunctionsStateBootstrapOperatorFactory
    extends AbstractStreamOperatorFactory<TaggedOperatorSubtaskState> {

  private final StateBootstrapFunctionRegistry stateBootstrapFunctionRegistry;
  private final long timestamp;
  private final Path savepointPath;

  public FunctionsStateBootstrapOperatorFactory(
      StateBootstrapFunctionRegistry stateBootstrapFunctionRegistry,
      long timestamp,
      Path savepointPath) {

    this.stateBootstrapFunctionRegistry = stateBootstrapFunctionRegistry;
    this.timestamp = timestamp;
    this.savepointPath = savepointPath;
  }

  @Override
  public <T extends StreamOperator<TaggedOperatorSubtaskState>> T createStreamOperator(
      StreamOperatorParameters<TaggedOperatorSubtaskState> streamOperatorParameters) {
    //noinspection unchecked
    return (T)
        new FunctionsStateBootstrapOperator(
            this.stateBootstrapFunctionRegistry, this.timestamp, this.savepointPath);
  }

  @Override
  public Class<? extends StreamOperator<TaggedOperatorSubtaskState>> getStreamOperatorClass(
      ClassLoader classLoader) {
    return FunctionsStateBootstrapOperator.class;
  }
}
