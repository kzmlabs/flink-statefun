// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;

class FlinkFunctionDispatcherMetricsTest {

  @Test
  void asyncOperationRegisteredAndCompletedAdjustInflightCounter() {
    RecordingMetricGroup group = new RecordingMetricGroup();
    FlinkFunctionDispatcherMetrics metrics = new FlinkFunctionDispatcherMetrics(group);

    metrics.asyncOperationRegistered();
    metrics.asyncOperationRegistered();
    metrics.asyncOperationRegistered();
    metrics.asyncOperationCompleted();

    Counter inflight = group.counters.get("inflightAsyncOps");
    assertThat(inflight).isNotNull();
    assertThat(inflight.getCount()).isEqualTo(2);
  }

  @Test
  void counterIsRegisteredUnderInflightAsyncOpsName() {
    RecordingMetricGroup group = new RecordingMetricGroup();

    new FlinkFunctionDispatcherMetrics(group);

    // Pin: the metric name `inflightAsyncOps` is the public dashboard contract — renaming
    // would break operator dashboards.
    assertThat(group.counters).containsKey("inflightAsyncOps");
  }

  @Test
  void rejectsNullMetricGroup() {
    assertThatThrownBy(() -> new FlinkFunctionDispatcherMetrics(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("operatorGroup");
  }

  /** A MetricGroup that records every counter registered, for assertion. */
  private static final class RecordingMetricGroup extends UnregisteredMetricsGroup {
    final Map<String, Counter> counters = new HashMap<>();

    @Override
    public Counter counter(String name) {
      Counter c = super.counter(name);
      counters.put(name, c);
      return c;
    }

    @Override
    public <C extends Counter> C counter(String name, C counter) {
      C c = super.counter(name, counter);
      counters.put(name, c);
      return c;
    }
  }

}
