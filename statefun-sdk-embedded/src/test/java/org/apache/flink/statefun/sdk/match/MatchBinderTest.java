// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.match;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins the matching contract of {@link MatchBinder}. Production code reaches the binder only via
 * {@link StatefulMatchFunction#invoke}, so these tests guard against silent precedence changes
 * during refactoring.
 */
class MatchBinderTest {

  @Test
  void typeOnlyPredicateMatchesByExactClassAndDispatchesAction() {
    MatchBinder binder = new MatchBinder();
    List<String> dispatched = new ArrayList<>();
    binder.predicate(String.class, (ctx, s) -> dispatched.add("string:" + s));

    binder.invoke(new NoOpContext(), "hello");

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

    binder.invoke(new NoOpContext(), "apple");
    binder.invoke(new NoOpContext(), "banana");

    assertThat(dispatched).containsExactly("conditional:apple", "type-only:banana");
  }

  @Test
  void multipleConditionalPredicatesAreCheckedInRegistrationOrder() {
    MatchBinder binder = new MatchBinder();
    List<Integer> dispatched = new ArrayList<>();
    binder.predicate(Integer.class, i -> i > 0, (ctx, i) -> dispatched.add(1));
    binder.predicate(Integer.class, i -> i > 10, (ctx, i) -> dispatched.add(2));
    binder.predicate(Integer.class, i -> i > 100, (ctx, i) -> dispatched.add(3));

    binder.invoke(new NoOpContext(), 1000);

    assertThat(dispatched).containsExactly(1);
  }

  @Test
  void unmatchedInputThrowsByDefault() {
    MatchBinder binder = new MatchBinder();
    binder.predicate(String.class, (ctx, s) -> {});

    assertThatThrownBy(() -> binder.invoke(new NoOpContext(), 42))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Don't know how to handle");
  }

  @Test
  void otherwiseClauseHandlesUnmatchedInputsInsteadOfThrowing() {
    MatchBinder binder = new MatchBinder();
    List<Object> defaulted = new ArrayList<>();
    binder.otherwise((ctx, msg) -> defaulted.add(msg));

    binder.invoke(new NoOpContext(), 42);
    binder.invoke(new NoOpContext(), "anything");

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

  /**
   * Dispatch uses {@code input.getClass()} for exact match: a registration for {@link Number}
   * does NOT catch {@link Integer}. Pin so users don't get a silent miss when expecting
   * inheritance dispatch.
   */
  @Test
  void typeOnlyMatchIsBasedOnExactClassNotInheritance() {
    MatchBinder binder = new MatchBinder();
    List<Object> hits = new ArrayList<>();
    binder.predicate(Number.class, (ctx, n) -> hits.add(n));

    assertThatThrownBy(() -> binder.invoke(new NoOpContext(), 42))
        .isInstanceOf(IllegalStateException.class);
    assertThat(hits).isEmpty();
  }

}
