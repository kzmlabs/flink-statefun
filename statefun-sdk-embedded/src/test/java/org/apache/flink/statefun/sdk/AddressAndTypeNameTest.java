// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class AddressAndTypeNameTest {

  @Test
  void addressEqualsBasedOnTypeAndId() {
    Address a = new Address(new FunctionType("ns", "fn"), "id-1");
    Address b = new Address(new FunctionType("ns", "fn"), "id-1");
    Address differentId = new Address(new FunctionType("ns", "fn"), "id-2");
    Address differentType = new Address(new FunctionType("ns", "other"), "id-1");

    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(differentId).isNotEqualTo(differentType);
  }

  @Test
  void addressToStringContainsAllThreeFields() {
    Address a = new Address(new FunctionType("ns", "fn"), "id-1");

    assertThat(a.toString()).contains("ns", "fn", "id-1");
  }

  @Test
  void addressRejectsNullTypeOrId() {
    assertThatThrownBy(() -> new Address(null, "id")).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new Address(new FunctionType("ns", "fn"), null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void addressEqualsToSelfAndNotNullOrUnrelated() {
    Address a = new Address(new FunctionType("ns", "fn"), "id-1");
    assertThat(a).isEqualTo(a).isNotNull().isNotEqualTo("string");
  }

  @Test
  void typeNameParseFromAcceptsSlashSeparatedString() {
    TypeName tn = TypeName.parseFrom("ns/fn");

    assertThat(tn.namespace()).isEqualTo("ns");
    assertThat(tn.name()).isEqualTo("fn");
    assertThat(tn.canonicalTypenameString()).isEqualTo("ns/fn");
  }

  @Test
  void typeNameParseFromRejectsMissingSlash() {
    assertThatThrownBy(() -> TypeName.parseFrom("nofn"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void typeNameParseFromRejectsTooManySlashes() {
    assertThatThrownBy(() -> TypeName.parseFrom("a/b/c"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void typeNameRejectsNullArguments() {
    assertThatThrownBy(() -> new TypeName(null, "n")).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new TypeName("ns", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void typeNameEqualityIsDeterminedByNamespaceAndName() {
    TypeName a = new TypeName("ns", "fn");
    TypeName b = new TypeName("ns", "fn");
    TypeName different = new TypeName("ns", "other");

    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(different);
  }

  @Test
  void typeNameToStringHasReadableForm() {
    assertThat(new TypeName("ns", "fn").toString()).contains("ns");
  }

  @Test
  void namespaceMatcherMatchesSameNamespaceAndRejectsOthers() {
    FunctionTypeNamespaceMatcher matcher = FunctionTypeNamespaceMatcher.targetNamespace("counter");

    assertThat(matcher.matches(new FunctionType("counter", "any"))).isTrue();
    assertThat(matcher.matches(new FunctionType("other", "any"))).isFalse();
  }

  @Test
  void namespaceMatcherEqualsBasedOnTargetNamespace() {
    FunctionTypeNamespaceMatcher a = FunctionTypeNamespaceMatcher.targetNamespace("counter");
    FunctionTypeNamespaceMatcher b = FunctionTypeNamespaceMatcher.targetNamespace("counter");
    FunctionTypeNamespaceMatcher c = FunctionTypeNamespaceMatcher.targetNamespace("other");

    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(c);
  }

  @Test
  void namespaceMatcherEqualsToSelfAndNotNullOrUnrelated() {
    FunctionTypeNamespaceMatcher a = FunctionTypeNamespaceMatcher.targetNamespace("x");
    assertThat(a).isEqualTo(a).isNotNull().isNotEqualTo("string");
  }

  @Test
  void namespaceMatcherToStringContainsTarget() {
    assertThat(FunctionTypeNamespaceMatcher.targetNamespace("counter").toString())
        .contains("counter");
  }

  @Test
  void namespaceMatcherRejectsNullNamespace() {
    assertThatThrownBy(() -> FunctionTypeNamespaceMatcher.targetNamespace(null))
        .isInstanceOf(NullPointerException.class);
  }
}
