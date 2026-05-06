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

import java.time.Duration;
import org.junit.jupiter.api.Test;

class AddressAndExpirationTest {

  // ----- Address -----

  @Test
  void addressAccessorsAndEquality() {
    TypeName fn = TypeName.typeNameOf("ns", "fn");
    Address a = new Address(fn, "id-1");
    Address b = new Address(fn, "id-1");
    Address differentId = new Address(fn, "id-2");

    assertThat(a.type(), is(equalTo(fn)));
    assertEquals("id-1", a.id());
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat(a, is(not(equalTo(differentId))));
  }

  @Test
  void addressRejectsNullArguments() {
    assertThrows(NullPointerException.class, () -> new Address(null, "id"));
    assertThrows(
        NullPointerException.class, () -> new Address(TypeName.typeNameOf("ns", "fn"), null));
  }

  @Test
  void addressToStringContainsAllThreeFields() {
    Address a = new Address(TypeName.typeNameOf("ns", "fn"), "id-1");
    assertThat(a.toString(), containsString("ns"));
    assertThat(a.toString(), containsString("fn"));
    assertThat(a.toString(), containsString("id-1"));
  }

  @Test
  void addressEqualsToSelfAndNotNullOrUnrelated() {
    Address a = new Address(TypeName.typeNameOf("ns", "fn"), "id-1");
    assertEquals(a, a);
    assertThat(a, is(not(equalTo(null))));
    assertThat((Object) a, is(not(equalTo((Object) "string"))));
  }

  // ----- Expiration -----

  @Test
  void expireAfterWritingProducesAfterWriteMode() {
    Expiration exp = Expiration.expireAfterWriting(Duration.ofMinutes(5));

    assertEquals(Expiration.Mode.AFTER_WRITE, exp.mode());
    assertEquals(Duration.ofMinutes(5), exp.duration());
  }

  @Test
  void expireAfterCallProducesAfterCallMode() {
    Expiration exp = Expiration.expireAfterCall(Duration.ofSeconds(30));

    assertEquals(Expiration.Mode.AFTER_CALL, exp.mode());
    assertEquals(Duration.ofSeconds(30), exp.duration());
  }

  @Test
  void expireAfterWithExplicitModeAcceptsAllModes() {
    Expiration none = Expiration.expireAfter(Duration.ofSeconds(1), Expiration.Mode.NONE);
    Expiration write =
        Expiration.expireAfter(Duration.ofSeconds(1), Expiration.Mode.AFTER_WRITE);
    Expiration call = Expiration.expireAfter(Duration.ofSeconds(1), Expiration.Mode.AFTER_CALL);

    assertEquals(Expiration.Mode.NONE, none.mode());
    assertEquals(Expiration.Mode.AFTER_WRITE, write.mode());
    assertEquals(Expiration.Mode.AFTER_CALL, call.mode());
  }

  @Test
  void noneFactoryReturnsZeroDurationNoneMode() {
    Expiration none = Expiration.none();

    assertEquals(Expiration.Mode.NONE, none.mode());
    assertEquals(Duration.ZERO, none.duration());
  }

  @Test
  void expirationConstructionRejectsNullDuration() {
    assertThrows(NullPointerException.class, () -> Expiration.expireAfterWriting(null));
    assertThrows(NullPointerException.class, () -> Expiration.expireAfterCall(null));
  }

  @Test
  void expirationToStringHasReadableFormWithModeAndDuration() {
    Expiration exp = Expiration.expireAfterWriting(Duration.ofSeconds(30));
    assertThat(exp.toString(), containsString("AFTER_WRITE"));
    assertThat(exp.toString(), containsString("PT30S"));
  }
}
