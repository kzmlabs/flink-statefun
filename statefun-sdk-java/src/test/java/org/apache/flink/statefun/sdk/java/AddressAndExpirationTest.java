// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class AddressAndExpirationTest {

  @Test
  void addressAccessorsAndEquality() {
    TypeName fn = TypeName.typeNameOf("ns", "fn");
    Address a = new Address(fn, "id-1");
    Address b = new Address(fn, "id-1");
    Address differentId = new Address(fn, "id-2");

    assertThat(a.type()).isEqualTo(fn);
    assertThat(a.id()).isEqualTo("id-1");
    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(differentId);
  }

  @Test
  void addressRejectsNullArguments() {
    assertThatThrownBy(() -> new Address(null, "id")).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new Address(TypeName.typeNameOf("ns", "fn"), null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void addressToStringContainsAllThreeFields() {
    Address a = new Address(TypeName.typeNameOf("ns", "fn"), "id-1");
    assertThat(a.toString()).contains("ns", "fn", "id-1");
  }

  @Test
  void addressEqualsToSelfAndNotNullOrUnrelated() {
    Address a = new Address(TypeName.typeNameOf("ns", "fn"), "id-1");
    assertThat(a).isEqualTo(a).isNotNull().isNotEqualTo("string");
  }

  @Test
  void expireAfterWritingProducesAfterWriteMode() {
    Expiration exp = Expiration.expireAfterWriting(Duration.ofMinutes(5));

    assertThat(exp.mode()).isEqualTo(Expiration.Mode.AFTER_WRITE);
    assertThat(exp.duration()).isEqualTo(Duration.ofMinutes(5));
  }

  @Test
  void expireAfterCallProducesAfterCallMode() {
    Expiration exp = Expiration.expireAfterCall(Duration.ofSeconds(30));

    assertThat(exp.mode()).isEqualTo(Expiration.Mode.AFTER_CALL);
    assertThat(exp.duration()).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void expireAfterWithExplicitModeAcceptsAllModes() {
    Expiration none = Expiration.expireAfter(Duration.ofSeconds(1), Expiration.Mode.NONE);
    Expiration write =
        Expiration.expireAfter(Duration.ofSeconds(1), Expiration.Mode.AFTER_WRITE);
    Expiration call = Expiration.expireAfter(Duration.ofSeconds(1), Expiration.Mode.AFTER_CALL);

    assertThat(none.mode()).isEqualTo(Expiration.Mode.NONE);
    assertThat(write.mode()).isEqualTo(Expiration.Mode.AFTER_WRITE);
    assertThat(call.mode()).isEqualTo(Expiration.Mode.AFTER_CALL);
  }

  @Test
  void noneFactoryReturnsZeroDurationNoneMode() {
    Expiration none = Expiration.none();

    assertThat(none.mode()).isEqualTo(Expiration.Mode.NONE);
    assertThat(none.duration()).isEqualTo(Duration.ZERO);
  }

  @Test
  void expirationConstructionRejectsNullDuration() {
    assertThatThrownBy(() -> Expiration.expireAfterWriting(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> Expiration.expireAfterCall(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> Expiration.expireAfter(null, Expiration.Mode.AFTER_WRITE))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void expirationConstructionRejectsNullMode() {
    assertThatThrownBy(() -> Expiration.expireAfter(Duration.ofSeconds(1), null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void expirationToStringHasReadableFormWithModeAndDuration() {
    Expiration exp = Expiration.expireAfterWriting(Duration.ofSeconds(30));
    assertThat(exp.toString()).contains("AFTER_WRITE", "PT30S");
  }
}
