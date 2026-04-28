// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.sdk.java.handler;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.slice.Slice;

public interface RequestReplyHandler {

  /**
   * Handles a {@code Stateful Functions} invocation.
   *
   * @param input a {@linkplain Slice} as received from the {@code StateFun} server.
   * @return a serialized representation of the side-effects to preform by the {@code StateFun}
   *     server, as the result of this invocation.
   */
  CompletableFuture<Slice> handle(Slice input);
}
