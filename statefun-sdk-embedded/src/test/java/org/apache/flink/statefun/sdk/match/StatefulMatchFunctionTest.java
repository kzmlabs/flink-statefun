// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.match;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.metrics.Metrics;
import org.junit.jupiter.api.Test;

class StatefulMatchFunctionTest {

  @Test
  void configureIsInvokedOnceLazilyOnFirstInvoke() {
    ConfigureCounter fn = new ConfigureCounter();

    fn.invoke(new NullContext(), "first");
    fn.invoke(new NullContext(), "second");
    fn.invoke(new NullContext(), "third");

    assertThat(fn.configureCalls).isEqualTo(1);
    assertThat(fn.dispatched).containsExactly("first", "second", "third");
  }

  @Test
  void unconfiguredFunctionThrowsForUnmatchedInput() {
    StatefulMatchFunction fn =
        new StatefulMatchFunction() {
          @Override
          public void configure(MatchBinder binder) {
            // No bindings — every input falls through to the default unhandled action.
          }
        };

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> fn.invoke(new NullContext(), 42))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Don't know how to handle");
  }

  /**
   * Concrete StatefulMatchFunction that records each invocation. Used to pin the lazy-init
   * contract — configure() must run exactly once across many invokes, even on the same instance.
   */
  private static final class ConfigureCounter extends StatefulMatchFunction {
    int configureCalls = 0;
    final List<String> dispatched = new ArrayList<>();

    @Override
    public void configure(MatchBinder binder) {
      configureCalls++;
      binder.predicate(String.class, (ctx, s) -> dispatched.add(s));
    }
  }

  /** No-op Context used for tests where the matcher doesn't interact with the context. */
  private static final class NullContext implements Context {
    @Override
    public Address self() {
      return null;
    }

    @Override
    public Address caller() {
      return null;
    }

    @Override
    public void send(Address to, Object message) {}

    @Override
    public <T> void send(EgressIdentifier<T> egress, T message) {}

    @Override
    public void sendAfter(Duration delay, Address to, Object message) {}

    @Override
    public void sendAfter(Duration delay, Address to, Object message, String cancellationToken) {}

    @Override
    public void cancelDelayedMessage(String cancellationToken) {}

    @Override
    public <M, T> void registerAsyncOperation(M metadata, CompletableFuture<T> future) {}

    @Override
    public Metrics metrics() {
      return null;
    }
  }
}
