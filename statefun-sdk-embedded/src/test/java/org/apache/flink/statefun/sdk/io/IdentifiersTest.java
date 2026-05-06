// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.io;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class IdentifiersTest {

  // ----- EgressIdentifier -----

  @Test
  void egressIdentifierAccessorsAndEquality() {
    EgressIdentifier<String> a = new EgressIdentifier<>("ns", "name", String.class);
    EgressIdentifier<String> b = new EgressIdentifier<>("ns", "name", String.class);
    EgressIdentifier<String> differentName = new EgressIdentifier<>("ns", "other", String.class);
    EgressIdentifier<Integer> differentType =
        new EgressIdentifier<>("ns", "name", Integer.class);

    assertThat(a.namespace(), equalTo("ns"));
    assertThat(a.name(), equalTo("name"));
    assertEquals(String.class, a.consumedType());
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat(a, is(not(equalTo(differentName))));
    assertThat(a, is(not(equalTo(differentType))));
  }

  @Test
  void egressIdentifierToStringContainsAllFields() {
    EgressIdentifier<String> id = new EgressIdentifier<>("ns", "name", String.class);

    assertThat(id.toString(), containsString("ns"));
    assertThat(id.toString(), containsString("name"));
    assertThat(id.toString(), containsString("String"));
  }

  @Test
  void egressIdentifierRejectsNullArguments() {
    assertThrows(
        NullPointerException.class, () -> new EgressIdentifier<>(null, "n", String.class));
    assertThrows(
        NullPointerException.class, () -> new EgressIdentifier<>("ns", null, String.class));
    assertThrows(NullPointerException.class, () -> new EgressIdentifier<>("ns", "n", null));
  }

  @Test
  void egressIdentifierEqualsToSelfAndNotNullOrUnrelated() {
    EgressIdentifier<String> id = new EgressIdentifier<>("ns", "name", String.class);
    assertEquals(id, id);
    assertThat(id, is(not(equalTo(null))));
    assertThat((Object) id, is(not(equalTo((Object) "string"))));
  }

  // ----- IngressIdentifier -----

  @Test
  void ingressIdentifierAccessorsAndEquality() {
    IngressIdentifier<String> a = new IngressIdentifier<>(String.class, "ns", "name");
    IngressIdentifier<String> b = new IngressIdentifier<>(String.class, "ns", "name");
    IngressIdentifier<String> differentName =
        new IngressIdentifier<>(String.class, "ns", "other");
    IngressIdentifier<Integer> differentType =
        new IngressIdentifier<>(Integer.class, "ns", "name");

    assertThat(a.namespace(), equalTo("ns"));
    assertThat(a.name(), equalTo("name"));
    assertEquals(String.class, a.producedType());
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat(a, is(not(equalTo(differentName))));
    assertThat(a, is(not(equalTo(differentType))));
  }

  @Test
  void ingressIdentifierToStringContainsAllFields() {
    IngressIdentifier<String> id = new IngressIdentifier<>(String.class, "ns", "name");

    assertThat(id.toString(), containsString("ns"));
    assertThat(id.toString(), containsString("name"));
  }

  @Test
  void ingressIdentifierRejectsNullArguments() {
    assertThrows(
        NullPointerException.class, () -> new IngressIdentifier<>(null, "ns", "n"));
    assertThrows(
        NullPointerException.class, () -> new IngressIdentifier<>(String.class, null, "n"));
    assertThrows(
        NullPointerException.class, () -> new IngressIdentifier<>(String.class, "ns", null));
  }

  @Test
  void ingressIdentifierEqualsToSelfAndNotNullOrUnrelated() {
    IngressIdentifier<String> id = new IngressIdentifier<>(String.class, "ns", "name");
    assertEquals(id, id);
    assertThat(id, is(not(equalTo(null))));
    assertThat((Object) id, is(not(equalTo((Object) "string"))));
  }
}
