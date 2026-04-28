// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.translation;

import java.io.Serializable;
import java.util.function.LongFunction;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;

final class CheckpointToMessage implements Serializable, LongFunction<Message> {

  private static final long serialVersionUID = 2L;

  private final MessageFactoryKey messageFactoryKey;
  private transient MessageFactory factory;

  CheckpointToMessage(MessageFactoryKey messageFactoryKey) {
    this.messageFactoryKey = messageFactoryKey;
  }

  @Override
  public Message apply(long checkpointId) {
    return factory().from(checkpointId);
  }

  private MessageFactory factory() {
    if (factory == null) {
      factory = MessageFactory.forKey(messageFactoryKey);
    }
    return factory;
  }
}
