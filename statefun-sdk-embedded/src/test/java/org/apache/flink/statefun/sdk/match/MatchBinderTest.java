// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.match;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.metrics.Metrics;
import org.junit.jupiter.api.Test;

class MatchBinderTest {

  // The MatchBinder constructor is package-private, so the test class lives in the same package
  // and uses `new MatchBinder()` directly. Production code never news up a binder — it goes
  // through StatefulMatchFunction.invoke → matcher.invoke. Tests pin the matching contract so a
  // future refactor can't silently change precedence.

  @Test
  void typeOnlyPredicateMatchesByExactClassAndDispatchesAction() {
    MatchBinder binder = new MatchBinder();
    List<String> dispatched = new ArrayList<>();
    binder.predicate(String.class, (ctx, s) -> dispatched.add("string:" + s));

    binder.invoke(new RecordingContext(), "hello");

    assertThat(dispatched).containsExactly("string:hello");
  }

  @Test
  void typeOnlyPredicateRejectsDuplicateRegistrationForSameType() {
    MatchBinder binder = new MatchBinder();
    binder.predicate(String.class, (ctx, s) -> {});

    assertThatThrownBy(() -> binder.predicate(String.class, (ctx, s) -> {}))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("catch all case for class");
  }

  @Test
  void typedPredicateWithConditionTakesPrecedenceOverTypeOnlyPredicate() {
    MatchBinder binder = new MatchBinder();
    List<String> dispatched = new ArrayList<>();
    binder.predicate(String.class, (ctx, s) -> dispatched.add("type-only:" + s));
    binder.predicate(
        String.class, s -> s.startsWith("a"), (ctx, s) -> dispatched.add("conditional:" + s));

    binder.invoke(new RecordingContext(), "apple"); // matches conditional
    binder.invoke(new RecordingContext(), "banana"); // falls through to type-only

    assertThat(dispatched).containsExactly("conditional:apple", "type-only:banana");
  }

  @Test
  void multipleConditionalPredicatesAreCheckedInRegistrationOrder() {
    MatchBinder binder = new MatchBinder();
    List<Integer> dispatched = new ArrayList<>();
    binder.predicate(Integer.class, i -> i > 0, (ctx, i) -> dispatched.add(1));
    binder.predicate(Integer.class, i -> i > 10, (ctx, i) -> dispatched.add(2));
    binder.predicate(Integer.class, i -> i > 100, (ctx, i) -> dispatched.add(3));

    binder.invoke(new RecordingContext(), 1000);

    // Even though all three predicates would match, the FIRST one wins.
    assertThat(dispatched).containsExactly(1);
  }

  @Test
  void unmatchedInputThrowsByDefault() {
    MatchBinder binder = new MatchBinder();
    binder.predicate(String.class, (ctx, s) -> {});

    assertThatThrownBy(() -> binder.invoke(new RecordingContext(), 42))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Don't know how to handle");
  }

  @Test
  void otherwiseClauseHandlesUnmatchedInputsInsteadOfThrowing() {
    MatchBinder binder = new MatchBinder();
    List<Object> defaulted = new ArrayList<>();
    binder.otherwise((ctx, msg) -> defaulted.add(msg));

    binder.invoke(new RecordingContext(), 42);
    binder.invoke(new RecordingContext(), "anything");

    assertThat(defaulted).containsExactly(42, "anything");
  }

  @Test
  void otherwiseRejectsDuplicateRegistration() {
    MatchBinder binder = new MatchBinder();
    binder.otherwise((ctx, msg) -> {});

    assertThatThrownBy(() -> binder.otherwise((ctx, msg) -> {}))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("only be one default action");
  }

  @Test
  void registrationApiRejectsNullArgs() {
    MatchBinder binder = new MatchBinder();
    assertThatThrownBy(() -> binder.predicate(null, (ctx, s) -> {}))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> binder.predicate(String.class, null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> binder.otherwise(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void typeOnlyMatchIsBasedOnExactClassNotInheritance() {
    // Pin: dispatch uses input.getClass() exact match — a registration for Number does NOT
    // catch Integer. Matters because users may expect inheritance dispatch.
    MatchBinder binder = new MatchBinder();
    List<Object> hits = new ArrayList<>();
    binder.predicate(Number.class, (ctx, n) -> hits.add(n));

    assertThatThrownBy(() -> binder.invoke(new RecordingContext(), 42))
        .isInstanceOf(IllegalStateException.class);
    assertThat(hits).isEmpty();
  }

  /** Minimal Context implementation that records nothing — invoke() doesn't use the context. */
  private static final class RecordingContext implements Context {
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
