// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    assertTrue(pos.isGroupOffsets());
    assertFalse(pos.isEarliest());
    assertFalse(pos.isLatest());
    assertFalse(pos.isSpecificOffsets());
    assertFalse(pos.isDate());
  }

  @Test
  void earliestExposesEarliestPredicateOnly() {
    KafkaIngressStartupPosition pos = KafkaIngressStartupPosition.fromEarliest();

    assertTrue(pos.isEarliest());
    assertFalse(pos.isGroupOffsets());
  }

  @Test
  void latestExposesLatestPredicateOnly() {
    KafkaIngressStartupPosition pos = KafkaIngressStartupPosition.fromLatest();

    assertTrue(pos.isLatest());
    assertFalse(pos.isEarliest());
  }

  @Test
  void specificOffsetsExposesSpecificOffsetsAndCarriesPayload() {
    Map<KafkaTopicPartition, Long> offsets = new HashMap<>();
    offsets.put(new KafkaTopicPartition("t", 0), 100L);
    offsets.put(new KafkaTopicPartition("t", 1), 200L);

    KafkaIngressStartupPosition pos = KafkaIngressStartupPosition.fromSpecificOffsets(offsets);

    assertTrue(pos.isSpecificOffsets());
    assertThat(pos.asSpecificOffsets().specificOffsets(), is(equalTo(offsets)));
  }

  @Test
  void specificOffsetsRejectsNullMap() {
    assertThrows(
        IllegalArgumentException.class, () -> KafkaIngressStartupPosition.fromSpecificOffsets(null));
  }

  @Test
  void specificOffsetsRejectsEmptyMap() {
    assertThrows(
        IllegalArgumentException.class,
        () -> KafkaIngressStartupPosition.fromSpecificOffsets(Collections.emptyMap()));
  }

  @Test
  void datePositionCarriesEpochMilli() {
    ZonedDateTime date = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    KafkaIngressStartupPosition pos = KafkaIngressStartupPosition.fromDate(date);

    assertTrue(pos.isDate());
    assertEquals(date.toInstant().toEpochMilli(), pos.asDate().epochMilli());
  }

  @Test
  void asSpecificOffsetsOnDatePositionThrows() {
    KafkaIngressStartupPosition date =
        KafkaIngressStartupPosition.fromDate(ZonedDateTime.now(ZoneOffset.UTC));

    assertThrows(IllegalStateException.class, date::asSpecificOffsets);
  }

  @Test
  void asDateOnGroupOffsetsThrows() {
    KafkaIngressStartupPosition pos = KafkaIngressStartupPosition.fromGroupOffsets();

    assertThrows(IllegalStateException.class, pos::asDate);
  }

  @Test
  void groupOffsetsEqualsAndHashCode() {
    KafkaIngressStartupPosition a = KafkaIngressStartupPosition.fromGroupOffsets();
    KafkaIngressStartupPosition b = KafkaIngressStartupPosition.fromGroupOffsets();

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat((Object) a, is(not(equalTo((Object) "string"))));
  }

  @Test
  void earliestEqualsAndHashCode() {
    KafkaIngressStartupPosition a = KafkaIngressStartupPosition.fromEarliest();
    KafkaIngressStartupPosition b = KafkaIngressStartupPosition.fromEarliest();

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void latestEqualsAndHashCode() {
    KafkaIngressStartupPosition a = KafkaIngressStartupPosition.fromLatest();
    KafkaIngressStartupPosition b = KafkaIngressStartupPosition.fromLatest();

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
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

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat(a, is(not(equalTo(c))));
  }

  @Test
  void datePositionEqualityRespectsDate() {
    ZonedDateTime t1 = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    ZonedDateTime t2 = ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    KafkaIngressStartupPosition a = KafkaIngressStartupPosition.fromDate(t1);
    KafkaIngressStartupPosition b = KafkaIngressStartupPosition.fromDate(t1);
    KafkaIngressStartupPosition c = KafkaIngressStartupPosition.fromDate(t2);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat(a, is(not(equalTo(c))));
  }
}
