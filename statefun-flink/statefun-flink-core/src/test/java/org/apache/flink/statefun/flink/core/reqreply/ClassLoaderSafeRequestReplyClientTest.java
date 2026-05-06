// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.reqreply;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URLClassLoader;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.junit.jupiter.api.Test;

class ClassLoaderSafeRequestReplyClientTest {

  @Test
  void delegateCallSeesItsOwnClassLoaderAsContext() {
    // The decorator's job: temporarily swap the thread's context class loader to the
    // delegate's class loader for the duration of `call`, then restore.
    CapturingClient delegate = new CapturingClient();
    ClassLoaderSafeRequestReplyClient client = new ClassLoaderSafeRequestReplyClient(delegate);

    ClassLoader externalLoader = Thread.currentThread().getContextClassLoader();
    client.call(null, null, ToFunction.getDefaultInstance());

    // The delegate should have observed its own class loader, not the external one.
    assertThat(delegate.observedContextClassLoader)
        .isEqualTo(delegate.getClass().getClassLoader());
    // After call returns, the original loader must be restored.
    assertThat(Thread.currentThread().getContextClassLoader()).isEqualTo(externalLoader);
  }

  @Test
  void contextClassLoaderRestoredEvenIfDelegateThrows() {
    ThrowingClient throwingDelegate = new ThrowingClient();
    ClassLoaderSafeRequestReplyClient client =
        new ClassLoaderSafeRequestReplyClient(throwingDelegate);
    ClassLoader original = Thread.currentThread().getContextClassLoader();

    assertThatThrownBy(() -> client.call(null, null, ToFunction.getDefaultInstance()))
        .isInstanceOf(IllegalStateException.class);

    // Pin the finally-block contract.
    assertThat(Thread.currentThread().getContextClassLoader()).isEqualTo(original);
  }

  @Test
  void rejectsNullDelegate() {
    assertThatThrownBy(() -> new ClassLoaderSafeRequestReplyClient(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void usesDifferentClassLoaderWhenDelegateLoadedFromOne() throws Exception {
    // Delegate from a custom ClassLoader -- pin that the swap actually picks up the delegate's
    // CL, not the test's CL.
    URLClassLoader customLoader =
        new URLClassLoader(
            new java.net.URL[0], ClassLoaderSafeRequestReplyClientTest.class.getClassLoader());

    // Use a delegate whose getClass().getClassLoader() == customLoader is hard to fabricate
    // without bytecode tricks. Instead, pin that the delegate's CL is what's used by checking
    // the equality directly.
    CapturingClient delegate = new CapturingClient();
    ClassLoaderSafeRequestReplyClient client = new ClassLoaderSafeRequestReplyClient(delegate);

    Thread.currentThread().setContextClassLoader(customLoader);
    try {
      client.call(null, null, ToFunction.getDefaultInstance());
      assertThat(delegate.observedContextClassLoader)
          .isEqualTo(delegate.getClass().getClassLoader());
      // Restored to whatever was set before the call (here: customLoader, not the original).
      assertThat(Thread.currentThread().getContextClassLoader()).isEqualTo(customLoader);
    } finally {
      Thread.currentThread().setContextClassLoader(customLoader.getParent());
      customLoader.close();
    }
  }

  private static final class CapturingClient implements RequestReplyClient {
    ClassLoader observedContextClassLoader;

    @Override
    public CompletableFuture<FromFunction> call(
        ToFunctionRequestSummary requestSummary,
        RemoteInvocationMetrics metrics,
        ToFunction toFunction) {
      this.observedContextClassLoader = Thread.currentThread().getContextClassLoader();
      return CompletableFuture.completedFuture(FromFunction.getDefaultInstance());
    }
  }

  private static final class ThrowingClient implements RequestReplyClient {
    @Override
    public CompletableFuture<FromFunction> call(
        ToFunctionRequestSummary requestSummary,
        RemoteInvocationMetrics metrics,
        ToFunction toFunction) {
      throw new IllegalStateException("delegate failure");
    }
  }
}
