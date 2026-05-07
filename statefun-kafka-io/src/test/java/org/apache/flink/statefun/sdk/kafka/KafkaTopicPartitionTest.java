// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class KafkaTopicPartitionTest {

  @Test
  void constructorRoundtripsTopicAndPartition() {
    KafkaTopicPartition tp = new KafkaTopicPartition("orders", 7);

    assertThat(tp.topic()).isEqualTo("orders");
    assertThat(tp.partition()).isEqualTo(7);
  }

  @Test
  void constructorRejectsNullTopic() {
    assertThatThrownBy(() -> new KafkaTopicPartition(null, 0))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructorRejectsNegativePartition() {
    assertThatThrownBy(() -> new KafkaTopicPartition("orders", -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("partition id");
  }

  @Test
  void fromStringParsesValidFormat() {
    KafkaTopicPartition tp = KafkaTopicPartition.fromString("orders/0");

    assertThat(tp.topic()).isEqualTo("orders");
    assertThat(tp.partition()).isZero();
  }

  @Test
  void fromStringPreservesSlashesInTopic() {
    // The parser splits on the LAST `/`, so a slash inside the topic is preserved as-is.
    KafkaTopicPartition tp = KafkaTopicPartition.fromString("foo/bar/baz/9");

    assertThat(tp.topic()).isEqualTo("foo/bar/baz");
    assertThat(tp.partition()).isEqualTo(9);
  }

  @Test
  void fromStringRejectsMissingPartition() {
    assertThatThrownBy(() -> KafkaTopicPartition.fromString("orders"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not conform");
  }

  @Test
  void fromStringRejectsLeadingSlashOnly() {
    assertThatThrownBy(() -> KafkaTopicPartition.fromString("/0"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void fromStringRejectsTrailingSlash() {
    assertThatThrownBy(() -> KafkaTopicPartition.fromString("orders/"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void fromStringRejectsNonNumericPartition() {
    assertThatThrownBy(() -> KafkaTopicPartition.fromString("orders/abc"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid topic partition definition");
  }

  @Test
  void fromStringRejectsNegativePartitionValue() {
    assertThatThrownBy(() -> KafkaTopicPartition.fromString("orders/-1"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("partition id is expected to be an integer");
  }

  @Test
  void fromStringRejectsNullInput() {
    assertThatThrownBy(() -> KafkaTopicPartition.fromString(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void equalsAndHashCodeIdentifyByTopicAndPartition() {
    KafkaTopicPartition a = new KafkaTopicPartition("orders", 1);
    KafkaTopicPartition aAgain = new KafkaTopicPartition("orders", 1);
    KafkaTopicPartition differentTopic = new KafkaTopicPartition("payments", 1);
    KafkaTopicPartition differentPartition = new KafkaTopicPartition("orders", 2);

    assertThat(a).isEqualTo(a).isEqualTo(aAgain);
    assertThat(a).isNotEqualTo(differentTopic).isNotEqualTo(differentPartition);
    assertThat(a).isNotEqualTo(null).isNotEqualTo("not a partition");
    assertThat(a.hashCode()).isEqualTo(aAgain.hashCode());
  }

  @Test
  void toStringIncludesTopicAndPartition() {
    KafkaTopicPartition tp = new KafkaTopicPartition("orders", 7);

    assertThat(tp.toString()).contains("orders").contains("7");
  }
}
