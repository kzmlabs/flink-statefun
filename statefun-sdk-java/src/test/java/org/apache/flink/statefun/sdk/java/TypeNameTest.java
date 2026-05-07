// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TypeNameTest {

  @Test
  void typeNameOfSplitsAndStores() {
    TypeName tn = TypeName.typeNameOf("io.test", "fn");

    assertThat(tn.namespace()).isEqualTo("io.test");
    assertThat(tn.name()).isEqualTo("fn");
    assertThat(tn.asTypeNameString()).isEqualTo("io.test/fn");
  }

  @Test
  void typeNameOfTrimsTrailingSlashFromNamespace() {
    // Pin: typeNameOf strips a single trailing "/" from the namespace before validating.
    TypeName tn = TypeName.typeNameOf("io.test/", "fn");

    assertThat(tn.namespace()).isEqualTo("io.test");
  }

  @Test
  void typeNameOfRejectsEmptyNamespace() {
    assertThatThrownBy(() -> TypeName.typeNameOf("", "fn"))
        .isInstanceOf(IllegalArgumentException.class);
    // Pin: empty after trailing-slash strip also fails ("/" → "" → empty).
    assertThatThrownBy(() -> TypeName.typeNameOf("/", "fn"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void typeNameOfRejectsEmptyName() {
    assertThatThrownBy(() -> TypeName.typeNameOf("io.test", ""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void typeNameOfRejectsNullArguments() {
    assertThatThrownBy(() -> TypeName.typeNameOf(null, "fn"))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> TypeName.typeNameOf("ns", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void typeNameFromStringSplitsOnLastSlash() {
    TypeName tn = TypeName.typeNameFromString("io.test/sub/fn");

    // The implementation splits at the LAST slash — namespace can contain slashes.
    assertThat(tn.namespace()).isEqualTo("io.test/sub");
    assertThat(tn.name()).isEqualTo("fn");
  }

  @Test
  void typeNameFromStringRejectsMissingSlash() {
    assertThatThrownBy(() -> TypeName.typeNameFromString("noslash"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void typeNameFromStringRejectsLeadingSlash() {
    // "/foo" -> last slash at pos 0 -> namespace would be empty; reject.
    assertThatThrownBy(() -> TypeName.typeNameFromString("/foo"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void typeNameFromStringRejectsTrailingSlash() {
    assertThatThrownBy(() -> TypeName.typeNameFromString("ns/"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void typeNameFromStringRejectsNullInput() {
    assertThatThrownBy(() -> TypeName.typeNameFromString(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void equalityIsBasedOnNamespaceAndName() {
    TypeName a = TypeName.typeNameOf("ns", "fn");
    TypeName b = TypeName.typeNameOf("ns", "fn");
    TypeName different = TypeName.typeNameOf("ns", "other");

    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(different);
  }

  @Test
  void equalsToSelfAndNotNullOrUnrelated() {
    TypeName a = TypeName.typeNameOf("ns", "fn");
    assertThat(a).isEqualTo(a).isNotNull().isNotEqualTo("string");
  }

  @Test
  void toStringIsReadable() {
    assertThat(TypeName.typeNameOf("ns", "fn").toString()).contains("ns", "fn");
  }
}
