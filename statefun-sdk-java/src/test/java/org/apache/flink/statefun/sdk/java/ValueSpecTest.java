// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.junit.jupiter.api.Test;

class ValueSpecTest {

  @Test
  void primitiveTypeBuildersProduceCorrespondingTypeNames() {
    assertEquals(Types.integerType().typeName(), ValueSpec.named("v").withIntType().typeName());
    assertEquals(Types.longType().typeName(), ValueSpec.named("v").withLongType().typeName());
    assertEquals(Types.floatType().typeName(), ValueSpec.named("v").withFloatType().typeName());
    assertEquals(Types.doubleType().typeName(), ValueSpec.named("v").withDoubleType().typeName());
    assertEquals(
        Types.stringType().typeName(), ValueSpec.named("v").withUtf8StringType().typeName());
    assertEquals(
        Types.booleanType().typeName(), ValueSpec.named("v").withBooleanType().typeName());
  }

  @Test
  void defaultExpirationIsNone() {
    ValueSpec<Integer> spec = ValueSpec.named("v").withIntType();

    assertEquals(Expiration.Mode.NONE, spec.expiration().mode());
  }

  @Test
  void thatExpireAfterWriteProducesAfterWriteExpiration() {
    ValueSpec<Integer> spec =
        ValueSpec.named("v").thatExpireAfterWrite(Duration.ofMinutes(5)).withIntType();

    assertEquals(Expiration.Mode.AFTER_WRITE, spec.expiration().mode());
    assertEquals(Duration.ofMinutes(5), spec.expiration().duration());
  }

  @Test
  void thatExpiresAfterCallProducesAfterCallExpiration() {
    ValueSpec<Integer> spec =
        ValueSpec.named("v").thatExpiresAfterCall(Duration.ofSeconds(30)).withIntType();

    assertEquals(Expiration.Mode.AFTER_CALL, spec.expiration().mode());
    assertEquals(Duration.ofSeconds(30), spec.expiration().duration());
  }

  @Test
  void nameRoundtripsThroughBuilder() {
    ValueSpec<Integer> spec = ValueSpec.named("counter").withIntType();
    assertThat(spec.name(), is(equalTo("counter")));
  }

  @Test
  void typeAccessorReturnsTheTypeFromBuilder() {
    ValueSpec<String> spec = ValueSpec.named("v").withUtf8StringType();

    assertThat(spec.type().typeName(), is(equalTo(Types.stringType().typeName())));
  }

  @Test
  void rejectsNullName() {
    assertThrows(NullPointerException.class, () -> ValueSpec.named(null));
  }

  @Test
  void rejectsNamesStartingWithDigit() {
    assertThrows(IllegalArgumentException.class, () -> ValueSpec.named("1foo"));
  }

  @Test
  void rejectsNamesStartingWithSpace() {
    assertThrows(IllegalArgumentException.class, () -> ValueSpec.named(" foo"));
  }

  @Test
  void allowsUnderscoreLeadingNames() {
    // Pin: leading underscore is valid; "_foo" is a legal state name.
    ValueSpec<Integer> spec = ValueSpec.named("_foo").withIntType();
    assertEquals("_foo", spec.name());
  }

  @Test
  void allowsLetterDigitUnderscoreInBody() {
    ValueSpec<Integer> spec = ValueSpec.named("foo_bar123").withIntType();
    assertEquals("foo_bar123", spec.name());
  }

  @Test
  void rejectsHyphenInBody() {
    assertThrows(IllegalArgumentException.class, () -> ValueSpec.named("foo-bar"));
  }

  @Test
  void rejectsSpaceInBody() {
    assertThrows(IllegalArgumentException.class, () -> ValueSpec.named("foo bar"));
  }

  @Test
  void rejectsDotInBody() {
    assertThrows(IllegalArgumentException.class, () -> ValueSpec.named("foo.bar"));
  }

  @Test
  void withCustomTypeRejectsNullType() {
    ValueSpec.Untyped untyped = new ValueSpec.Untyped("v");
    assertThrows(NullPointerException.class, () -> untyped.withCustomType(null));
  }
}
