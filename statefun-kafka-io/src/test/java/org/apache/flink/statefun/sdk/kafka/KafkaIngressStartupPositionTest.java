// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaIngressStartupPositionTest {

  @Test
  void groupOffsetsExposesGroupOffsetsPredicateOnly() {
    KafkaIngressStartupPosition pos = KafkaIngressStartupPosition.fromGroupOffsets();

    assertThat(pos.isGroupOffsets()).isTrue();
    assertThat(pos.isEarliest()).isFalse();
    assertThat(pos.isLatest()).isFalse();
    assertThat(pos.isSpecificOffsets()).isFalse();
    assertThat(pos.isDate()).isFalse();
  }

  @Test
  void earliestExposesEarliestPredicateOnly() {
    KafkaIngressStartupPosition pos = KafkaIngressStartupPosition.fromEarliest();

    assertThat(pos.isEarliest()).isTrue();
    assertThat(pos.isGroupOffsets()).isFalse();
  }

  @Test
  void latestExposesLatestPredicateOnly() {
    KafkaIngressStartupPosition pos = KafkaIngressStartupPosition.fromLatest();

    assertThat(pos.isLatest()).isTrue();
    assertThat(pos.isEarliest()).isFalse();
  }

  @Test
  void specificOffsetsExposesSpecificOffsetsAndCarriesPayload() {
    Map<KafkaTopicPartition, Long> offsets = new HashMap<>();
    offsets.put(new KafkaTopicPartition("t", 0), 100L);
    offsets.put(new KafkaTopicPartition("t", 1), 200L);

    KafkaIngressStartupPosition pos = KafkaIngressStartupPosition.fromSpecificOffsets(offsets);

    assertThat(pos.isSpecificOffsets()).isTrue();
    assertThat(pos.asSpecificOffsets().specificOffsets()).isEqualTo(offsets);
  }

  @Test
  void specificOffsetsRejectsNullMap() {
    assertThatThrownBy(() -> KafkaIngressStartupPosition.fromSpecificOffsets(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void specificOffsetsRejectsEmptyMap() {
    assertThatThrownBy(
            () -> KafkaIngressStartupPosition.fromSpecificOffsets(Collections.emptyMap()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void datePositionCarriesEpochMilli() {
    ZonedDateTime date = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    KafkaIngressStartupPosition pos = KafkaIngressStartupPosition.fromDate(date);

    assertThat(pos.isDate()).isTrue();
    assertThat(pos.asDate().epochMilli()).isEqualTo(date.toInstant().toEpochMilli());
  }

  @Test
  void asSpecificOffsetsOnDatePositionThrows() {
    KafkaIngressStartupPosition date =
        KafkaIngressStartupPosition.fromDate(ZonedDateTime.now(ZoneOffset.UTC));

    assertThatThrownBy(date::asSpecificOffsets).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void asDateOnGroupOffsetsThrows() {
    KafkaIngressStartupPosition pos = KafkaIngressStartupPosition.fromGroupOffsets();

    assertThatThrownBy(pos::asDate).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void groupOffsetsEqualsAndHashCode() {
    KafkaIngressStartupPosition a = KafkaIngressStartupPosition.fromGroupOffsets();
    KafkaIngressStartupPosition b = KafkaIngressStartupPosition.fromGroupOffsets();

    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo("string");
  }

  @Test
  void earliestEqualsAndHashCode() {
    KafkaIngressStartupPosition a = KafkaIngressStartupPosition.fromEarliest();
    KafkaIngressStartupPosition b = KafkaIngressStartupPosition.fromEarliest();

    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
  }

  @Test
  void latestEqualsAndHashCode() {
    KafkaIngressStartupPosition a = KafkaIngressStartupPosition.fromLatest();
    KafkaIngressStartupPosition b = KafkaIngressStartupPosition.fromLatest();

    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
  }

  @Test
  void specificOffsetsEqualityRespectsMapContent() {
    Map<KafkaTopicPartition, Long> map1 = new HashMap<>();
    map1.put(new KafkaTopicPartition("t", 0), 1L);
    Map<KafkaTopicPartition, Long> map2 = new HashMap<>();
    map2.put(new KafkaTopicPartition("t", 0), 1L);
    Map<KafkaTopicPartition, Long> different = new HashMap<>();
    different.put(new KafkaTopicPartition("t", 0), 2L);

    KafkaIngressStartupPosition a = KafkaIngressStartupPosition.fromSpecificOffsets(map1);
    KafkaIngressStartupPosition b = KafkaIngressStartupPosition.fromSpecificOffsets(map2);
    KafkaIngressStartupPosition c = KafkaIngressStartupPosition.fromSpecificOffsets(different);

    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(c);
  }

  @Test
  void datePositionEqualityRespectsDate() {
    ZonedDateTime t1 = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    ZonedDateTime t2 = ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    KafkaIngressStartupPosition a = KafkaIngressStartupPosition.fromDate(t1);
    KafkaIngressStartupPosition b = KafkaIngressStartupPosition.fromDate(t1);
    KafkaIngressStartupPosition c = KafkaIngressStartupPosition.fromDate(t2);

    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(c);
  }
}
