// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class KafkaProducerSemanticTest {

  @Test
  void atLeastOncePredicates() {
    KafkaProducerSemantic s = KafkaProducerSemantic.atLeastOnce();

    assertThat(s.isAtLeastOnceSemantic()).isTrue();
    assertThat(s.isExactlyOnceSemantic()).isFalse();
    assertThat(s.isNoSemantic()).isFalse();
    assertThat(s.asAtLeastOnceSemantic()).isSameAs(s);
  }

  @Test
  void noSemanticPredicates() {
    KafkaProducerSemantic s = KafkaProducerSemantic.none();

    assertThat(s.isNoSemantic()).isTrue();
    assertThat(s.isAtLeastOnceSemantic()).isFalse();
    assertThat(s.isExactlyOnceSemantic()).isFalse();
    assertThat(s.asNoSemantic()).isSameAs(s);
  }

  @Test
  void exactlyOncePredicates() {
    KafkaProducerSemantic s = KafkaProducerSemantic.exactlyOnce(Duration.ofMinutes(5));

    assertThat(s.isExactlyOnceSemantic()).isTrue();
    assertThat(s.isAtLeastOnceSemantic()).isFalse();
    assertThat(s.isNoSemantic()).isFalse();
    assertThat(s.asExactlyOnceSemantic().transactionTimeout()).isEqualTo(Duration.ofMinutes(5));
  }

  @Test
  void exactlyOnceRejectsZeroTimeout() {
    assertThatThrownBy(() -> KafkaProducerSemantic.exactlyOnce(Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void exactlyOnceRejectsNullTimeout() {
    assertThatThrownBy(() -> KafkaProducerSemantic.exactlyOnce(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void asExactlyOnceOnAtLeastOnceClassCastFails() {
    KafkaProducerSemantic s = KafkaProducerSemantic.atLeastOnce();
    // The contract is "caller checks predicate first" — pin the cast failure mode.
    assertThatThrownBy(s::asExactlyOnceSemantic).isInstanceOf(ClassCastException.class);
  }
}
