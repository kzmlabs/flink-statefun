// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.backpressure;

import org.apache.flink.annotation.Internal;
import org.apache.flink.statefun.flink.core.metrics.FunctionTypeMetrics;
import org.apache.flink.statefun.sdk.Context;

@Internal
public interface InternalContext extends Context {

  /**
   * Signals the runtime to stop invoking the currently executing function with new input until at
   * least one {@link org.apache.flink.statefun.sdk.AsyncOperationResult} belonging to this function
   * would be delivered.
   *
   * <p>NOTE: If a function would request to block without actually registering any async operations
   * either previously or during its current invocation, then it would remain blocked. Since this is
   * an internal API to be used by the remote functions we don't do anything to prevent that.
   *
   * <p>If we would like it to be a part of the SDK then we would have to make sure that we track
   * every async operation registered per each address.
   */
  void awaitAsyncOperationComplete();

  /**
   * Returns the metrics handle for the current invoked function's type.
   *
   * @return the metrics handle for the current invoked function's type.
   */
  FunctionTypeMetrics functionTypeMetrics();
}
