// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TypeNameTest {

  @Test
  void typeNameOfSplitsAndStores() {
    TypeName tn = TypeName.typeNameOf("io.test", "fn");

    assertEquals("io.test", tn.namespace());
    assertEquals("fn", tn.name());
    assertEquals("io.test/fn", tn.asTypeNameString());
  }

  @Test
  void typeNameOfTrimsTrailingSlashFromNamespace() {
    // Pin: typeNameOf strips a single trailing "/" from the namespace before validating.
    TypeName tn = TypeName.typeNameOf("io.test/", "fn");

    assertEquals("io.test", tn.namespace());
  }

  @Test
  void typeNameOfRejectsEmptyNamespace() {
    assertThrows(IllegalArgumentException.class, () -> TypeName.typeNameOf("", "fn"));
    // Pin: empty after trailing-slash strip also fails ("/" → "" → empty).
    assertThrows(IllegalArgumentException.class, () -> TypeName.typeNameOf("/", "fn"));
  }

  @Test
  void typeNameOfRejectsEmptyName() {
    assertThrows(IllegalArgumentException.class, () -> TypeName.typeNameOf("io.test", ""));
  }

  @Test
  void typeNameOfRejectsNullArguments() {
    assertThrows(NullPointerException.class, () -> TypeName.typeNameOf(null, "fn"));
    assertThrows(NullPointerException.class, () -> TypeName.typeNameOf("ns", null));
  }

  @Test
  void typeNameFromStringSplitsOnLastSlash() {
    TypeName tn = TypeName.typeNameFromString("io.test/sub/fn");

    // The implementation splits at the LAST slash — namespace can contain slashes.
    assertEquals("io.test/sub", tn.namespace());
    assertEquals("fn", tn.name());
  }

  @Test
  void typeNameFromStringRejectsMissingSlash() {
    assertThrows(IllegalArgumentException.class, () -> TypeName.typeNameFromString("noslash"));
  }

  @Test
  void typeNameFromStringRejectsLeadingSlash() {
    // "/foo" -> last slash at pos 0 -> namespace would be empty; reject.
    assertThrows(IllegalArgumentException.class, () -> TypeName.typeNameFromString("/foo"));
  }

  @Test
  void typeNameFromStringRejectsTrailingSlash() {
    assertThrows(IllegalArgumentException.class, () -> TypeName.typeNameFromString("ns/"));
  }

  @Test
  void typeNameFromStringRejectsNullInput() {
    assertThrows(NullPointerException.class, () -> TypeName.typeNameFromString(null));
  }

  @Test
  void equalityIsBasedOnNamespaceAndName() {
    TypeName a = TypeName.typeNameOf("ns", "fn");
    TypeName b = TypeName.typeNameOf("ns", "fn");
    TypeName different = TypeName.typeNameOf("ns", "other");

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat(a, is(not(equalTo(different))));
  }

  @Test
  void equalsToSelfAndNotNullOrUnrelated() {
    TypeName a = TypeName.typeNameOf("ns", "fn");
    assertEquals(a, a);
    assertThat(a, is(not(equalTo(null))));
    assertThat((Object) a, is(not(equalTo((Object) "string"))));
  }

  @Test
  void toStringIsReadable() {
    assertThat(TypeName.typeNameOf("ns", "fn").toString(), containsString("ns"));
    assertThat(TypeName.typeNameOf("ns", "fn").toString(), containsString("fn"));
  }
}
