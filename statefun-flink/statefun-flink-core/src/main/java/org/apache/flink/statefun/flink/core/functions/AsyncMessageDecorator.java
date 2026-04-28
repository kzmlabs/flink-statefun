// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.functions;

import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.Nullable;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.AsyncOperationResult.Status;

/**
 * Wraps the original {@link Message} where it's payload is the user supplied metadata associated
 * with an async operation.
 */
final class AsyncMessageDecorator<T> implements Message {
  private final PendingAsyncOperations pendingAsyncOperations;
  private final long futureId;
  private final Message message;
  private final Throwable throwable;
  private final T result;
  private final boolean restored;

  AsyncMessageDecorator(
      PendingAsyncOperations pendingAsyncOperations,
      long futureId,
      Message message,
      T result,
      Throwable throwable) {
    this.futureId = futureId;
    this.pendingAsyncOperations = pendingAsyncOperations;
    this.message = message;
    this.throwable = throwable;
    this.result = result;
    this.restored = false;
  }

  AsyncMessageDecorator(
      PendingAsyncOperations asyncOperationState, Long futureId, Message metadataMessage) {
    this.futureId = futureId;
    this.pendingAsyncOperations = asyncOperationState;
    this.message = metadataMessage;
    this.throwable = null;
    this.result = null;
    this.restored = true;
  }

  @Nullable
  @Override
  public Address source() {
    return message.source();
  }

  @Override
  public Address target() {
    return message.target();
  }

  @Override
  public Object payload(MessageFactory context, ClassLoader targetClassLoader) {
    final Status status;
    if (restored) {
      status = Status.UNKNOWN;
    } else if (throwable == null) {
      status = Status.SUCCESS;
    } else {
      status = Status.FAILURE;
    }
    Object metadata = message.payload(context, targetClassLoader);
    return new AsyncOperationResult<>(metadata, status, result, throwable);
  }

  @Override
  public OptionalLong isBarrierMessage() {
    return OptionalLong.empty();
  }

  @Override
  public Optional<String> cancellationToken() {
    return message.cancellationToken();
  }

  @Override
  public void postApply() {
    pendingAsyncOperations.remove(source(), futureId);
  }

  @Override
  public Message copy(MessageFactory context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeTo(MessageFactory context, DataOutputView target) {
    throw new UnsupportedOperationException();
  }
}
