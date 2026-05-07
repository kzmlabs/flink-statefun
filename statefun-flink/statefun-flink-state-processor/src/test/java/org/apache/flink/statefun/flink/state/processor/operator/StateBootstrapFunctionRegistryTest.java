// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.state.processor.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.statefun.flink.state.processor.StateBootstrapFunction;
import org.apache.flink.statefun.flink.state.processor.StateBootstrapFunctionProvider;
import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.jupiter.api.Test;

class StateBootstrapFunctionRegistryTest {

  private static final FunctionType FT_A = new FunctionType("ns", "a");
  private static final FunctionType FT_B = new FunctionType("ns", "b");

  @Test
  void newRegistryHasZeroRegistrations() {
    StateBootstrapFunctionRegistry registry = new StateBootstrapFunctionRegistry();

    assertThat(registry.numRegistrations()).isZero();
  }

  @Test
  void registerSingleProviderCountsAsOne() {
    StateBootstrapFunctionRegistry registry = new StateBootstrapFunctionRegistry();

    registry.register(FT_A, new NoopProvider());

    assertThat(registry.numRegistrations()).isEqualTo(1);
  }

  @Test
  void registerDistinctFunctionTypesAccumulates() {
    StateBootstrapFunctionRegistry registry = new StateBootstrapFunctionRegistry();

    registry.register(FT_A, new NoopProvider());
    registry.register(FT_B, new NoopProvider());

    assertThat(registry.numRegistrations()).isEqualTo(2);
  }

  @Test
  void registerSameFunctionTypeTwiceFailsLoudly() {
    StateBootstrapFunctionRegistry registry = new StateBootstrapFunctionRegistry();
    registry.register(FT_A, new NoopProvider());

    // Pin: silently overwriting a previous registration would be a real bug — we'd lose the
    // user's first bootstrap function provider with no warning.
    assertThatThrownBy(() -> registry.register(FT_A, new NoopProvider()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("previously defined");
  }

  @Test
  void registerRejectsNullFunctionType() {
    StateBootstrapFunctionRegistry registry = new StateBootstrapFunctionRegistry();

    assertThatThrownBy(() -> registry.register(null, new NoopProvider()))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void registerRejectsNullProvider() {
    StateBootstrapFunctionRegistry registry = new StateBootstrapFunctionRegistry();

    assertThatThrownBy(() -> registry.register(FT_A, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void getBootstrapFunctionBeforeInitializeFailsLoudly() {
    StateBootstrapFunctionRegistry registry = new StateBootstrapFunctionRegistry();
    registry.register(FT_A, new NoopProvider());

    assertThatThrownBy(() -> registry.getBootstrapFunction(FT_A))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("must be initialized first");
  }

  private static final class NoopProvider implements StateBootstrapFunctionProvider {
    @Override
    public StateBootstrapFunction bootstrapFunctionOfType(FunctionType type) {
      throw new UnsupportedOperationException();
    }
  }
}
