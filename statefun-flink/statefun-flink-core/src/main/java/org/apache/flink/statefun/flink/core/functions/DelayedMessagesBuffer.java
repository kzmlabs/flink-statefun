// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.functions;

import java.util.OptionalLong;
import java.util.function.Consumer;
import org.apache.flink.statefun.flink.core.message.Message;

interface DelayedMessagesBuffer {

  /** Add a message to be fired at a specific timestamp */
  void add(Message message, long untilTimestamp);

  /** Apply @fn for each delayed message that is meant to be fired at @timestamp. */
  void forEachMessageAt(long timestamp, Consumer<Message> fn);

  /**
   * @param token a message cancellation token to delete.
   * @return an optional timestamp that this message was meant to be fired at. The timestamp will be
   *     present only if this message was the last message registered to fire at that timestamp.
   *     (hence: safe to clear any underlying timer)
   */
  OptionalLong removeMessageByCancellationToken(String token);
}
