// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk;

/** Provides instances of {@link StatefulFunction}s for a given {@link FunctionType}. */
public interface StatefulFunctionProvider {

  /**
   * Creates a {@link StatefulFunction} instance for the given {@link FunctionType},
   *
   * @param type the type of function to create an instance for.
   * @return an instance of a function for the given type.
   */
  StatefulFunction functionOfType(FunctionType type);
}
