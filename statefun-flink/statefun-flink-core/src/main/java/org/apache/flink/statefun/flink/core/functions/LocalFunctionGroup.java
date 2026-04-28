// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.functions;

import java.util.ArrayDeque;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.FunctionType;

final class LocalFunctionGroup {
  private final FunctionRepository repository;
  private final ApplyingContext context;
  /**
   * pending is a queue of pairs (LiveFunction, Message) as enqueued via {@link #enqueue(Message)}.
   * In order to avoid an object pool, or redundant allocations we store these pairs linearly one
   * after another in the queue.
   */
  private final ArrayDeque<Object> pending;

  @Inject
  LocalFunctionGroup(
      @Label("function-repository") FunctionRepository repository,
      @Label("applying-context") ApplyingContext context) {
    this.pending = new ArrayDeque<>(4096);
    this.repository = Objects.requireNonNull(repository);
    this.context = Objects.requireNonNull(context);
  }

  void enqueue(Message message) {
    FunctionType targetType = message.target().type();
    LiveFunction fn = repository.get(targetType);
    pending.addLast(fn);
    pending.addLast(message);
  }

  boolean processNextEnvelope() {
    Object fn = pending.pollFirst();
    if (fn == null) {
      return false;
    }
    LiveFunction liveFunction = (LiveFunction) fn;
    Message message = (Message) pending.pollFirst();
    context.apply(liveFunction, message);
    return true;
  }
}
