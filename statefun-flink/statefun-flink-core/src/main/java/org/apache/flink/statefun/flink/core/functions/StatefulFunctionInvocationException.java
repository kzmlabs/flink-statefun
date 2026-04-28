// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.functions;

import org.apache.flink.statefun.sdk.FunctionType;

/** A Stateful Functions exception that may be thrown when invoking a function. */
public final class StatefulFunctionInvocationException extends RuntimeException {

  public StatefulFunctionInvocationException(FunctionType functionType, Throwable cause) {
    super(
        String.format("An error occurred when attempting to invoke function %s.", functionType),
        cause);
  }
}
