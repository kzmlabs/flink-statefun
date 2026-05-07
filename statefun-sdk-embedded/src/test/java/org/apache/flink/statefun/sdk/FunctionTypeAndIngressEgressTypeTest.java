// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class FunctionTypeAndIngressEgressTypeTest {

  @Test
  void functionTypeAccessorsAndEquality() {
    FunctionType a = new FunctionType("ns", "fn");
    FunctionType b = new FunctionType("ns", "fn");
    FunctionType different = new FunctionType("ns", "other");

    assertThat(a.namespace()).isEqualTo("ns");
    assertThat(a.name()).isEqualTo("fn");
    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(different);
  }

  @Test
  void functionTypeRejectsNullNamespaceOrName() {
    assertThatThrownBy(() -> new FunctionType(null, "fn"))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new FunctionType("ns", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void functionTypeToStringContainsBothFields() {
    assertThat(new FunctionType("ns", "fn").toString()).contains("ns", "fn");
  }

  @Test
  void functionTypeEqualsToSelfAndNotNullOrUnrelated() {
    FunctionType ft = new FunctionType("ns", "fn");
    assertThat(ft).isEqualTo(ft).isNotNull().isNotEqualTo("string");
  }

  @Test
  void ingressTypeAccessorsAndEquality() {
    IngressType a = new IngressType("io.test", "kafka");
    IngressType b = new IngressType("io.test", "kafka");
    IngressType different = new IngressType("io.test", "kinesis");

    assertThat(a.namespace()).isEqualTo("io.test");
    assertThat(a.type()).isEqualTo("kafka");
    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(different);
  }

  @Test
  void ingressTypeRejectsNullNamespaceOrType() {
    assertThatThrownBy(() -> new IngressType(null, "kafka"))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new IngressType("io.test", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void ingressTypeEqualsToSelfAndNotNullOrUnrelated() {
    IngressType i = new IngressType("io.test", "kafka");
    assertThat(i).isEqualTo(i).isNotNull().isNotEqualTo("string");
  }

  @Test
  void egressTypeAccessorsAndEquality() {
    EgressType a = new EgressType("io.test", "kafka");
    EgressType b = new EgressType("io.test", "kafka");
    EgressType different = new EgressType("io.test", "kinesis");

    assertThat(a.namespace()).isEqualTo("io.test");
    assertThat(a.type()).isEqualTo("kafka");
    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(different);
  }

  @Test
  void egressTypeRejectsNullNamespaceOrType() {
    assertThatThrownBy(() -> new EgressType(null, "kafka"))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new EgressType("io.test", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void egressTypeToStringPinsCurrentIngressTypeTypo() {
    // TODO: EgressType.toString() reports "IngressType(...)" due to a copy-paste typo
    // inherited from upstream. Pinning the literal prefix here so a future fix is
    // intentional — when corrected, flip to .contains("EgressType").
    EgressType e = new EgressType("io.test", "kafka");
    assertThat(e.toString()).contains("IngressType(", "io.test", "kafka");
  }
}
