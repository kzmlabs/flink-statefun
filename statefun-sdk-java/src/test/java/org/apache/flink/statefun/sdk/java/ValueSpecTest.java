// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.junit.jupiter.api.Test;

class ValueSpecTest {

  @Test
  void primitiveTypeBuildersProduceCorrespondingTypeNames() {
    assertThat(ValueSpec.named("v").withIntType().typeName())
        .isEqualTo(Types.integerType().typeName());
    assertThat(ValueSpec.named("v").withLongType().typeName())
        .isEqualTo(Types.longType().typeName());
    assertThat(ValueSpec.named("v").withFloatType().typeName())
        .isEqualTo(Types.floatType().typeName());
    assertThat(ValueSpec.named("v").withDoubleType().typeName())
        .isEqualTo(Types.doubleType().typeName());
    assertThat(ValueSpec.named("v").withUtf8StringType().typeName())
        .isEqualTo(Types.stringType().typeName());
    assertThat(ValueSpec.named("v").withBooleanType().typeName())
        .isEqualTo(Types.booleanType().typeName());
  }

  @Test
  void defaultExpirationIsNone() {
    ValueSpec<Integer> spec = ValueSpec.named("v").withIntType();

    assertThat(spec.expiration().mode()).isEqualTo(Expiration.Mode.NONE);
  }

  @Test
  void thatExpireAfterWriteProducesAfterWriteExpiration() {
    ValueSpec<Integer> spec =
        ValueSpec.named("v").thatExpireAfterWrite(Duration.ofMinutes(5)).withIntType();

    assertThat(spec.expiration().mode()).isEqualTo(Expiration.Mode.AFTER_WRITE);
    assertThat(spec.expiration().duration()).isEqualTo(Duration.ofMinutes(5));
  }

  @Test
  void thatExpiresAfterCallProducesAfterCallExpiration() {
    ValueSpec<Integer> spec =
        ValueSpec.named("v").thatExpiresAfterCall(Duration.ofSeconds(30)).withIntType();

    assertThat(spec.expiration().mode()).isEqualTo(Expiration.Mode.AFTER_CALL);
    assertThat(spec.expiration().duration()).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void nameRoundtripsThroughBuilder() {
    ValueSpec<Integer> spec = ValueSpec.named("counter").withIntType();
    assertThat(spec.name()).isEqualTo("counter");
  }

  @Test
  void typeAccessorReturnsTheTypeFromBuilder() {
    ValueSpec<String> spec = ValueSpec.named("v").withUtf8StringType();

    assertThat(spec.type().typeName()).isEqualTo(Types.stringType().typeName());
  }

  @Test
  void rejectsNullName() {
    assertThatThrownBy(() -> ValueSpec.named(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void rejectsNamesStartingWithDigit() {
    assertThatThrownBy(() -> ValueSpec.named("1foo"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsNamesStartingWithSpace() {
    assertThatThrownBy(() -> ValueSpec.named(" foo"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void allowsUnderscoreLeadingNames() {
    // Pin: leading underscore is valid; "_foo" is a legal state name.
    ValueSpec<Integer> spec = ValueSpec.named("_foo").withIntType();
    assertThat(spec.name()).isEqualTo("_foo");
  }

  @Test
  void allowsLetterDigitUnderscoreInBody() {
    ValueSpec<Integer> spec = ValueSpec.named("foo_bar123").withIntType();
    assertThat(spec.name()).isEqualTo("foo_bar123");
  }

  @Test
  void rejectsHyphenInBody() {
    assertThatThrownBy(() -> ValueSpec.named("foo-bar"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsSpaceInBody() {
    assertThatThrownBy(() -> ValueSpec.named("foo bar"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsDotInBody() {
    assertThatThrownBy(() -> ValueSpec.named("foo.bar"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void withCustomTypeRejectsNullType() {
    ValueSpec.Untyped untyped = new ValueSpec.Untyped("v");
    assertThatThrownBy(() -> untyped.withCustomType(null))
        .isInstanceOf(NullPointerException.class);
  }
}
