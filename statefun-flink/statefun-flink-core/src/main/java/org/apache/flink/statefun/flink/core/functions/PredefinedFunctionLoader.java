// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.functions;

import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.flink.common.SetContextClassLoader;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

/** An {@link FunctionLoader} that has a predefined set of {@link StatefulFunctionProvider}s. */
final class PredefinedFunctionLoader implements FunctionLoader {
  private final Map<FunctionType, StatefulFunctionProvider> functionProviders;
  private final Map<String, StatefulFunctionProvider> namespaceFunctionProviders;

  @Inject
  PredefinedFunctionLoader(
      @Label("function-providers") Map<FunctionType, StatefulFunctionProvider> functionProviders,
      @Label("namespace-function-providers")
          Map<String, StatefulFunctionProvider> namespaceFunctionProviders) {
    this.functionProviders = Objects.requireNonNull(functionProviders);
    this.namespaceFunctionProviders = Objects.requireNonNull(namespaceFunctionProviders);
  }

  @Override
  public StatefulFunction load(FunctionType functionType) {
    Objects.requireNonNull(functionType);
    final StatefulFunctionProvider provider = getFunctionProviderOrThrow(functionType);
    final StatefulFunction statefulFunction = load(provider, functionType);
    if (statefulFunction == null) {
      throw new IllegalStateException(
          "A provider for a type " + functionType + " has produced a NULL function");
    }
    return statefulFunction;
  }

  private StatefulFunctionProvider getFunctionProviderOrThrow(FunctionType functionType) {
    StatefulFunctionProvider provider = functionProviders.get(functionType);
    if (provider != null) {
      return provider;
    }
    provider = namespaceFunctionProviders.get(functionType.namespace());
    if (provider != null) {
      return provider;
    }
    throw new IllegalArgumentException("Cannot find a provider for type " + functionType);
  }

  private static StatefulFunction load(
      StatefulFunctionProvider provider, FunctionType functionType) {
    try (SetContextClassLoader ignored = new SetContextClassLoader(provider)) {
      return provider.functionOfType(functionType);
    }
  }
}
