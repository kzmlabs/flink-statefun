// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.jsonmodule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class FormatVersionTest {

  @Test
  void allKnownVersionsParseToCorrespondingEnum() {
    assertThat(FormatVersion.fromString("1.0")).isEqualTo(FormatVersion.v1_0);
    assertThat(FormatVersion.fromString("2.0")).isEqualTo(FormatVersion.v2_0);
    assertThat(FormatVersion.fromString("3.0")).isEqualTo(FormatVersion.v3_0);
    assertThat(FormatVersion.fromString("3.1")).isEqualTo(FormatVersion.v3_1);
  }

  @Test
  void toStringReturnsTheVersionString() {
    // Pin: toString must produce the wire-format string used in YAML — this is the inverse of
    // fromString and is relied on for error messages.
    assertThat(FormatVersion.v3_0.toString()).isEqualTo("3.0");
    assertThat(FormatVersion.v3_1.toString()).isEqualTo("3.1");
  }

  @Test
  void fromStringRejectsUnrecognizedVersion() {
    assertThatThrownBy(() -> FormatVersion.fromString("4.0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unrecognized format version");
  }

  @Test
  void compareToOrderingMatchesSemverOrdering() {
    // The legacy-vs-supported gate uses compareTo with v3_0 — pin the ordering invariant.
    assertThat(FormatVersion.v1_0.compareTo(FormatVersion.v3_0)).isLessThan(0);
    assertThat(FormatVersion.v2_0.compareTo(FormatVersion.v3_0)).isLessThan(0);
    assertThat(FormatVersion.v3_0.compareTo(FormatVersion.v3_0)).isZero();
    assertThat(FormatVersion.v3_1.compareTo(FormatVersion.v3_0)).isGreaterThan(0);
  }
}
