// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.reqreply;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;

/**
 * Decorator for a {@link RequestReplyClient} that makes sure we always use the correct classloader.
 * This is required since client implementation may be user provided.
 */
public final class ClassLoaderSafeRequestReplyClient implements RequestReplyClient {

  private final ClassLoader delegateClassLoader;
  private final RequestReplyClient delegate;

  public ClassLoaderSafeRequestReplyClient(RequestReplyClient delegate) {
    this.delegate = Objects.requireNonNull(delegate);
    this.delegateClassLoader = delegate.getClass().getClassLoader();
  }

  @Override
  public CompletableFuture<FromFunction> call(
      ToFunctionRequestSummary requestSummary,
      RemoteInvocationMetrics metrics,
      ToFunction toFunction) {
    final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();

    try {
      Thread.currentThread().setContextClassLoader(delegateClassLoader);
      return delegate.call(requestSummary, metrics, toFunction);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }
}
