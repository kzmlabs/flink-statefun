// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AddressAndTypeNameTest {

  // ------- Address ---------

  @Test
  void addressEqualsBasedOnTypeAndId() {
    Address a = new Address(new FunctionType("ns", "fn"), "id-1");
    Address b = new Address(new FunctionType("ns", "fn"), "id-1");
    Address differentId = new Address(new FunctionType("ns", "fn"), "id-2");
    Address differentType = new Address(new FunctionType("ns", "other"), "id-1");

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat(a, is(not(equalTo(differentId))));
    assertThat(a, is(not(equalTo(differentType))));
  }

  @Test
  void addressToStringContainsAllThreeFields() {
    Address a = new Address(new FunctionType("ns", "fn"), "id-1");

    assertThat(a.toString(), containsString("ns"));
    assertThat(a.toString(), containsString("fn"));
    assertThat(a.toString(), containsString("id-1"));
  }

  @Test
  void addressRejectsNullTypeOrId() {
    assertThrows(NullPointerException.class, () -> new Address(null, "id"));
    assertThrows(
        NullPointerException.class, () -> new Address(new FunctionType("ns", "fn"), null));
  }

  @Test
  void addressEqualsToSelfAndNotNullOrUnrelated() {
    Address a = new Address(new FunctionType("ns", "fn"), "id-1");
    assertEquals(a, a);
    assertThat(a, is(not(equalTo(null))));
    assertThat((Object) a, is(not(equalTo((Object) "string"))));
  }

  // ------- TypeName ---------

  @Test
  void typeNameParseFromAcceptsSlashSeparatedString() {
    TypeName tn = TypeName.parseFrom("ns/fn");

    assertThat(tn.namespace(), equalTo("ns"));
    assertThat(tn.name(), equalTo("fn"));
    assertThat(tn.canonicalTypenameString(), equalTo("ns/fn"));
  }

  @Test
  void typeNameParseFromRejectsMissingSlash() {
    assertThrows(IllegalArgumentException.class, () -> TypeName.parseFrom("nofn"));
  }

  @Test
  void typeNameParseFromRejectsTooManySlashes() {
    assertThrows(IllegalArgumentException.class, () -> TypeName.parseFrom("a/b/c"));
  }

  @Test
  void typeNameRejectsNullArguments() {
    assertThrows(NullPointerException.class, () -> new TypeName(null, "n"));
    assertThrows(NullPointerException.class, () -> new TypeName("ns", null));
  }

  @Test
  void typeNameEqualityIsDeterminedByNamespaceAndName() {
    TypeName a = new TypeName("ns", "fn");
    TypeName b = new TypeName("ns", "fn");
    TypeName different = new TypeName("ns", "other");

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat(a, is(not(equalTo(different))));
  }

  @Test
  void typeNameToStringHasReadableForm() {
    assertThat(new TypeName("ns", "fn").toString(), containsString("ns"));
  }

  // ------- FunctionTypeNamespaceMatcher ---------

  @Test
  void namespaceMatcherMatchesSameNamespaceAndRejectsOthers() {
    FunctionTypeNamespaceMatcher matcher = FunctionTypeNamespaceMatcher.targetNamespace("counter");

    assertTrue(matcher.matches(new FunctionType("counter", "any")));
    assertEquals(false, matcher.matches(new FunctionType("other", "any")));
  }

  @Test
  void namespaceMatcherEqualsBasedOnTargetNamespace() {
    FunctionTypeNamespaceMatcher a = FunctionTypeNamespaceMatcher.targetNamespace("counter");
    FunctionTypeNamespaceMatcher b = FunctionTypeNamespaceMatcher.targetNamespace("counter");
    FunctionTypeNamespaceMatcher c = FunctionTypeNamespaceMatcher.targetNamespace("other");

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat(a, is(not(equalTo(c))));
  }

  @Test
  void namespaceMatcherEqualsToSelfAndNotNullOrUnrelated() {
    FunctionTypeNamespaceMatcher a = FunctionTypeNamespaceMatcher.targetNamespace("x");
    assertEquals(a, a);
    assertThat(a, is(not(equalTo(null))));
    assertThat((Object) a, is(not(equalTo((Object) "string"))));
  }

  @Test
  void namespaceMatcherToStringContainsTarget() {
    assertThat(
        FunctionTypeNamespaceMatcher.targetNamespace("counter").toString(),
        containsString("counter"));
  }

  @Test
  void namespaceMatcherRejectsNullNamespace() {
    assertThrows(NullPointerException.class, () -> FunctionTypeNamespaceMatcher.targetNamespace(null));
  }
}
