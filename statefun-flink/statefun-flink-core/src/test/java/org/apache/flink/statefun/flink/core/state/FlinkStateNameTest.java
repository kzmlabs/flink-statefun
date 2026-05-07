// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.state;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.jupiter.api.Test;

class FlinkStateNameTest {

  /**
   * The flinkStateName format is part of Flink savepoint compatibility — changing it would
   * break checkpoint restore on existing payloads. Pin the format with concrete examples so
   * a refactor surface this as a test failure instead of a silent state-loss in production.
   */
  @Test
  void flinkStateNameUsesNamespaceDotNameDotPersistedValueName() {
    String stateName =
        FlinkState.flinkStateName(new FunctionType("orders", "validator"), "outstanding");

    assertThat(stateName).isEqualTo("orders.validator.outstanding");
  }

  @Test
  void emptyPersistedValueNameStillProducesValidStateName() {
    String stateName = FlinkState.flinkStateName(new FunctionType("ns", "fn"), "");

    // Trailing dot is documented behavior — empty persisted-value names are valid (they show
    // up for "anonymous" PersistedValues), and the resulting state name still has the
    // namespace.fn. prefix that the keyed backend matches on.
    assertThat(stateName).isEqualTo("ns.fn.");
  }

  @Test
  void specialCharactersInFunctionTypeFlowThroughVerbatim() {
    String stateName =
        FlinkState.flinkStateName(new FunctionType("io.payments", "fn-1"), "balance");

    assertThat(stateName).isEqualTo("io.payments.fn-1.balance");
  }
}
