// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class KafkaProducerSemanticTest {

  @Test
  void atLeastOncePredicates() {
    KafkaProducerSemantic s = KafkaProducerSemantic.atLeastOnce();

    assertTrue(s.isAtLeastOnceSemantic());
    assertFalse(s.isExactlyOnceSemantic());
    assertFalse(s.isNoSemantic());
    assertThat(s.asAtLeastOnceSemantic(), is(s));
  }

  @Test
  void noSemanticPredicates() {
    KafkaProducerSemantic s = KafkaProducerSemantic.none();

    assertTrue(s.isNoSemantic());
    assertFalse(s.isAtLeastOnceSemantic());
    assertFalse(s.isExactlyOnceSemantic());
    assertThat(s.asNoSemantic(), is(s));
  }

  @Test
  void exactlyOncePredicates() {
    KafkaProducerSemantic s = KafkaProducerSemantic.exactlyOnce(Duration.ofMinutes(5));

    assertTrue(s.isExactlyOnceSemantic());
    assertFalse(s.isAtLeastOnceSemantic());
    assertFalse(s.isNoSemantic());
    assertEquals(Duration.ofMinutes(5), s.asExactlyOnceSemantic().transactionTimeout());
  }

  @Test
  void exactlyOnceRejectsZeroTimeout() {
    assertThrows(
        IllegalArgumentException.class,
        () -> KafkaProducerSemantic.exactlyOnce(Duration.ZERO));
  }

  @Test
  void exactlyOnceRejectsNullTimeout() {
    assertThrows(NullPointerException.class, () -> KafkaProducerSemantic.exactlyOnce(null));
  }

  @Test
  void asExactlyOnceOnAtLeastOnceClassCastFails() {
    KafkaProducerSemantic s = KafkaProducerSemantic.atLeastOnce();
    // The contract is "caller checks predicate first" — pin the cast failure mode.
    assertThrows(ClassCastException.class, s::asExactlyOnceSemantic);
  }
}
