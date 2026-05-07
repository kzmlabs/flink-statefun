// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class IdentifiersTest {

  @Test
  void egressIdentifierAccessorsAndEquality() {
    EgressIdentifier<String> a = new EgressIdentifier<>("ns", "name", String.class);
    EgressIdentifier<String> b = new EgressIdentifier<>("ns", "name", String.class);
    EgressIdentifier<String> differentName = new EgressIdentifier<>("ns", "other", String.class);
    EgressIdentifier<Integer> differentType =
        new EgressIdentifier<>("ns", "name", Integer.class);

    assertThat(a.namespace()).isEqualTo("ns");
    assertThat(a.name()).isEqualTo("name");
    assertThat(a.consumedType()).isEqualTo(String.class);
    assertThat(a)
        .isEqualTo(b)
        .hasSameHashCodeAs(b)
        .isNotEqualTo(differentName)
        .isNotEqualTo(differentType);
  }

  @Test
  void egressIdentifierToStringContainsAllFields() {
    EgressIdentifier<String> id = new EgressIdentifier<>("ns", "name", String.class);

    assertThat(id.toString()).contains("ns", "name", "String");
  }

  @Test
  void egressIdentifierRejectsNullArguments() {
    assertThatThrownBy(() -> new EgressIdentifier<>(null, "n", String.class))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new EgressIdentifier<>("ns", null, String.class))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new EgressIdentifier<>("ns", "n", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void egressIdentifierEqualsToSelfAndNotNullOrUnrelated() {
    EgressIdentifier<String> id = new EgressIdentifier<>("ns", "name", String.class);
    assertThat(id).isEqualTo(id).isNotNull().isNotEqualTo("string");
  }

  @Test
  void ingressIdentifierAccessorsAndEquality() {
    IngressIdentifier<String> a = new IngressIdentifier<>(String.class, "ns", "name");
    IngressIdentifier<String> b = new IngressIdentifier<>(String.class, "ns", "name");
    IngressIdentifier<String> differentName =
        new IngressIdentifier<>(String.class, "ns", "other");
    IngressIdentifier<Integer> differentType =
        new IngressIdentifier<>(Integer.class, "ns", "name");

    assertThat(a.namespace()).isEqualTo("ns");
    assertThat(a.name()).isEqualTo("name");
    assertThat(a.producedType()).isEqualTo(String.class);
    assertThat(a)
        .isEqualTo(b)
        .hasSameHashCodeAs(b)
        .isNotEqualTo(differentName)
        .isNotEqualTo(differentType);
  }

  @Test
  void ingressIdentifierToStringContainsAllFields() {
    IngressIdentifier<String> id = new IngressIdentifier<>(String.class, "ns", "name");

    assertThat(id.toString()).contains("ns", "name");
  }

  @Test
  void ingressIdentifierRejectsNullArguments() {
    assertThatThrownBy(() -> new IngressIdentifier<>(null, "ns", "n"))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new IngressIdentifier<>(String.class, null, "n"))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new IngressIdentifier<>(String.class, "ns", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void ingressIdentifierEqualsToSelfAndNotNullOrUnrelated() {
    IngressIdentifier<String> id = new IngressIdentifier<>(String.class, "ns", "name");
    assertThat(id).isEqualTo(id).isNotNull().isNotEqualTo("string");
  }
}
