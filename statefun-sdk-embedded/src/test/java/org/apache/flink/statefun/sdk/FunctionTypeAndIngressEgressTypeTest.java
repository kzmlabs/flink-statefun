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

import org.junit.jupiter.api.Test;

class FunctionTypeAndIngressEgressTypeTest {

  @Test
  void functionTypeAccessorsAndEquality() {
    FunctionType a = new FunctionType("ns", "fn");
    FunctionType b = new FunctionType("ns", "fn");
    FunctionType different = new FunctionType("ns", "other");

    assertThat(a.namespace(), equalTo("ns"));
    assertThat(a.name(), equalTo("fn"));
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat(a, is(not(equalTo(different))));
  }

  @Test
  void functionTypeRejectsNullNamespaceOrName() {
    assertThrows(NullPointerException.class, () -> new FunctionType(null, "fn"));
    assertThrows(NullPointerException.class, () -> new FunctionType("ns", null));
  }

  @Test
  void functionTypeToStringContainsBothFields() {
    assertThat(new FunctionType("ns", "fn").toString(), containsString("ns"));
    assertThat(new FunctionType("ns", "fn").toString(), containsString("fn"));
  }

  @Test
  void functionTypeEqualsToSelfAndNotNullOrUnrelated() {
    FunctionType ft = new FunctionType("ns", "fn");
    assertEquals(ft, ft);
    assertThat(ft, is(not(equalTo(null))));
    assertThat((Object) ft, is(not(equalTo((Object) "string"))));
  }

  @Test
  void ingressTypeAccessorsAndEquality() {
    IngressType a = new IngressType("io.test", "kafka");
    IngressType b = new IngressType("io.test", "kafka");
    IngressType different = new IngressType("io.test", "kinesis");

    assertThat(a.namespace(), equalTo("io.test"));
    assertThat(a.type(), equalTo("kafka"));
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat(a, is(not(equalTo(different))));
  }

  @Test
  void ingressTypeRejectsNullNamespaceOrType() {
    assertThrows(NullPointerException.class, () -> new IngressType(null, "kafka"));
    assertThrows(NullPointerException.class, () -> new IngressType("io.test", null));
  }

  @Test
  void ingressTypeEqualsToSelfAndNotNullOrUnrelated() {
    IngressType i = new IngressType("io.test", "kafka");
    assertEquals(i, i);
    assertThat(i, is(not(equalTo(null))));
    assertThat((Object) i, is(not(equalTo((Object) "string"))));
  }

  @Test
  void egressTypeAccessorsAndEquality() {
    EgressType a = new EgressType("io.test", "kafka");
    EgressType b = new EgressType("io.test", "kafka");
    EgressType different = new EgressType("io.test", "kinesis");

    assertThat(a.namespace(), equalTo("io.test"));
    assertThat(a.type(), equalTo("kafka"));
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat(a, is(not(equalTo(different))));
  }

  @Test
  void egressTypeRejectsNullNamespaceOrType() {
    assertThrows(NullPointerException.class, () -> new EgressType(null, "kafka"));
    assertThrows(NullPointerException.class, () -> new EgressType("io.test", null));
  }

  @Test
  void egressTypeToStringPinsCurrentIngressTypeTypo() {
    // TODO: EgressType.toString() reports "IngressType(...)" due to a copy-paste typo
    // inherited from upstream. Pinning the literal prefix here so a future fix is
    // intentional — when corrected, flip to containsString("EgressType").
    EgressType e = new EgressType("io.test", "kafka");
    assertThat(e.toString(), containsString("IngressType("));
    assertThat(e.toString(), containsString("io.test"));
    assertThat(e.toString(), containsString("kafka"));
  }
}
