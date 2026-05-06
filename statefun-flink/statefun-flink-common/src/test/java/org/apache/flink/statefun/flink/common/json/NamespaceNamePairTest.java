// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.common.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class NamespaceNamePairTest {

  @Test
  void splitsAtLastSlashWhenSingleSegmentInEachSide() {
    NamespaceNamePair pair = NamespaceNamePair.from("counter/increment");

    assertThat(pair.namespace()).isEqualTo("counter");
    assertThat(pair.name()).isEqualTo("increment");
  }

  @Test
  void splitsAtLastSlashWhenNamespaceContainsSlashes() {
    // The contract is "split at the LAST slash" — earlier slashes are part of the namespace.
    NamespaceNamePair pair = NamespaceNamePair.from("io.test/payments/process");

    assertThat(pair.namespace()).isEqualTo("io.test/payments");
    assertThat(pair.name()).isEqualTo("process");
  }

  @Test
  void rejectsPureNameWithNoSlash() {
    assertThatThrownBy(() -> NamespaceNamePair.from("noseparator"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("<namespace>/<name>");
  }

  @Test
  void rejectsLeadingSlashWithEmptyNamespace() {
    // "/name" -> last slash at pos 0 — namespace would be empty.
    assertThatThrownBy(() -> NamespaceNamePair.from("/foo"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsTrailingSlashWithEmptyName() {
    // "ns/" -> last slash is the final char — name would be empty.
    assertThatThrownBy(() -> NamespaceNamePair.from("counter/"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsEmptyString() {
    assertThatThrownBy(() -> NamespaceNamePair.from(""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsNullInput() {
    assertThatThrownBy(() -> NamespaceNamePair.from(null))
        .isInstanceOf(NullPointerException.class);
  }
}
