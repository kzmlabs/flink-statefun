// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.match;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class StatefulMatchFunctionTest {

  @Test
  void configureIsInvokedOnceLazilyOnFirstInvoke() {
    ConfigureCounter fn = new ConfigureCounter();

    fn.invoke(new NoOpContext(), "first");
    fn.invoke(new NoOpContext(), "second");
    fn.invoke(new NoOpContext(), "third");

    assertThat(fn.configureCalls).isEqualTo(1);
    assertThat(fn.dispatched).containsExactly("first", "second", "third");
  }

  @Test
  void unconfiguredFunctionThrowsForUnmatchedInput() {
    StatefulMatchFunction fn =
        new StatefulMatchFunction() {
          @Override
          public void configure(MatchBinder binder) {}
        };

    assertThatThrownBy(() -> fn.invoke(new NoOpContext(), 42))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Don't know how to handle");
  }

  /**
   * Concrete {@link StatefulMatchFunction} recording each invocation to pin the lazy-init
   * contract: {@code configure()} must run exactly once across many invokes on the same instance.
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
}
