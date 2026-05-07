// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.statefun.sdk.metrics.Metrics;
import org.junit.jupiter.api.Test;

class FlinkUserMetricsTest {

  @Test
  void counterReturnsAnSdkCounterThatPropagatesToTheUnderlyingFlinkCounter() {
    CountingMetricGroup group = new CountingMetricGroup();
    Metrics metrics = new FlinkUserMetrics(group);

    org.apache.flink.statefun.sdk.metrics.Counter sdkCounter = metrics.counter("requests");
    sdkCounter.inc(1);
    sdkCounter.inc(5);
    sdkCounter.dec(2);

    Counter underlying = group.counters.get("requests");
    assertThat(underlying).isNotNull();
    assertThat(underlying.getCount()).isEqualTo(4);
  }

  @Test
  void counterReturnsCachedInstanceOnSecondLookupForSameName() {
    Metrics metrics = new FlinkUserMetrics(new CountingMetricGroup());

    org.apache.flink.statefun.sdk.metrics.Counter first = metrics.counter("c");
    org.apache.flink.statefun.sdk.metrics.Counter second = metrics.counter("c");

    // The wrapper map caches by name — same name returns same SDK wrapper instance, so the
    // user's mental model "I always get the same counter" holds.
    assertThat(first).isSameAs(second);
  }

  @Test
  void differentNamesProduceIndependentCountersWithNoCrossTalk() {
    CountingMetricGroup group = new CountingMetricGroup();
    Metrics metrics = new FlinkUserMetrics(group);

    org.apache.flink.statefun.sdk.metrics.Counter a = metrics.counter("a");
    org.apache.flink.statefun.sdk.metrics.Counter b = metrics.counter("b");

    a.inc(1);
    b.inc(10);
    a.inc(2);

    assertThat(a).isNotSameAs(b);
    assertThat(group.counters.get("a").getCount()).isEqualTo(3);
    assertThat(group.counters.get("b").getCount()).isEqualTo(10);
  }

  @Test
  void counterRejectsNullName() {
    Metrics metrics = new FlinkUserMetrics(new CountingMetricGroup());

    assertThatThrownBy(() -> metrics.counter(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void rejectsNullMetricGroup() {
    assertThatThrownBy(() -> new FlinkUserMetrics(null)).isInstanceOf(NullPointerException.class);
  }

  /** A MetricGroup that records every counter registered. */
  private static final class CountingMetricGroup extends UnregisteredMetricsGroup {
    final java.util.Map<String, Counter> counters = new java.util.HashMap<>();

    @Override
    public <C extends Counter> C counter(String name, C counter) {
      C c = super.counter(name, counter);
      counters.put(name, c);
      return c;
    }
  }
}
