// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.state.processor;

import java.io.Serializable;
import org.apache.flink.statefun.sdk.FunctionType;

/** Provides instances of {@link StateBootstrapFunction}s for a given {@link FunctionType}. */
public interface StateBootstrapFunctionProvider extends Serializable {

  /**
   * Creates a {@link StateBootstrapFunction} instance for the given {@link FunctionType},
   *
   * @param type the type of function to create a boostrap function instance for.
   * @return an instance of a bootstrap function for the given type.
   */
  StateBootstrapFunction bootstrapFunctionOfType(FunctionType type);
}
