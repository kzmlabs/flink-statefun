// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.statefun.sdk.metrics.Metrics;
import org.junit.jupiter.api.Test;

class FlinkFunctionTypeMetricsTest {

  @Test
  void registersAllExpectedCountersHistogramsAndMeters() {
    RecordingMetricGroup group = new RecordingMetricGroup();
    FlinkFunctionTypeMetrics m = new FlinkFunctionTypeMetrics(group, new FlinkUserMetrics(group));

    // Drive every metric so registration is observed.
    m.incomingMessage();
    m.outgoingLocalMessage();
    m.outgoingRemoteMessage();
    m.outgoingEgressMessage();
    m.blockedAddress();
    m.unblockedAddress();
    m.asyncOperationRegistered();
    m.asyncOperationCompleted();
    m.appendBacklogMessages(3);
    m.consumeBacklogMessages(2);
    m.remoteInvocationFailures();
    m.remoteInvocationLatency(123L);

    assertThat(group.counters)
        .containsKeys(
            "in",
            "outLocal",
            "outRemote",
            "outEgress",
            "numBlockedAddress",
            "inflightAsyncOps",
            "numBacklog",
            "remoteInvocationFailures");
    // The "metered" counters all have a sibling rate meter, suffixed Rate.
    assertThat(group.meters)
        .containsKeys("inRate", "outLocalRate", "outRemoteRate", "outEgressRate", "remoteInvocationFailuresRate");
    assertThat(group.histograms).containsKey("remoteInvocationLatency");
  }

  @Test
  void incomingAndOutgoingDoNotCrossTalk() {
    RecordingMetricGroup group = new RecordingMetricGroup();
    FlinkFunctionTypeMetrics m = new FlinkFunctionTypeMetrics(group, new FlinkUserMetrics(group));

    m.incomingMessage();
    m.outgoingLocalMessage();
    m.outgoingLocalMessage();

    assertThat(group.counters.get("in").getCount()).isEqualTo(1);
    assertThat(group.counters.get("outLocal").getCount()).isEqualTo(2);
    assertThat(group.counters.get("outRemote").getCount()).isZero();
  }

  @Test
  void backlogMessageCounterFloorsAtZeroForNonNegativeCounter() {
    RecordingMetricGroup group = new RecordingMetricGroup();
    FlinkFunctionTypeMetrics m = new FlinkFunctionTypeMetrics(group, new FlinkUserMetrics(group));

    m.appendBacklogMessages(2);
    m.consumeBacklogMessages(10); // pretend we drained more than was added

    // numBacklog uses NonNegativeCounter — must floor at zero, not go negative.
    assertThat(group.counters.get("numBacklog").getCount()).isZero();
  }

  @Test
  void blockedAddressInDecBalancesOut() {
    RecordingMetricGroup group = new RecordingMetricGroup();
    FlinkFunctionTypeMetrics m = new FlinkFunctionTypeMetrics(group, new FlinkUserMetrics(group));

    m.blockedAddress();
    m.blockedAddress();
    m.unblockedAddress();

    assertThat(group.counters.get("numBlockedAddress").getCount()).isEqualTo(1);
  }

  @Test
  void rejectsNullScopedMetrics() {
    assertThatThrownBy(() -> new FlinkFunctionTypeMetrics(new RecordingMetricGroup(), null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void exposesScopedMetricsBackToTheCaller() {
    RecordingMetricGroup group = new RecordingMetricGroup();
    Metrics scoped = new FlinkUserMetrics(group);
    FlinkFunctionTypeMetrics m = new FlinkFunctionTypeMetrics(group, scoped);

    assertThat(m.functionTypeScopedMetrics()).isSameAs(scoped);
  }

  @Test
  void factoryProducesScopedMetricsByNamespaceAndName() {
    RecordingMetricGroup parent = new RecordingMetricGroup();
    FlinkFuncionTypeMetricsFactory factory = new FlinkFuncionTypeMetricsFactory(parent);

    FunctionTypeMetrics m =
        factory.forType(new org.apache.flink.statefun.sdk.FunctionType("ns", "fn"));

    assertThat(m).isNotNull();
    assertThat(m.functionTypeScopedMetrics()).isNotNull();
    // The factory creates a nested metric group structure: parent / ns / fn.
    assertThat(parent.subgroupsByName).containsKey("ns");
    RecordingMetricGroup nsGroup = parent.subgroupsByName.get("ns");
    assertThat(nsGroup.subgroupsByName).containsKey("fn");
  }

  @Test
  void factoryRejectsNullParentGroup() {
    assertThatThrownBy(() -> new FlinkFuncionTypeMetricsFactory(null))
        .isInstanceOf(NullPointerException.class);
  }

  /**
   * A MetricGroup that records every metric registered on it, including counters, meters,
   * histograms, and any nested group it spawns. Used to pin metric naming + meter pairing.
   */
  private static final class RecordingMetricGroup extends UnregisteredMetricsGroup {
    final Map<String, Counter> counters = new HashMap<>();
    final Map<String, Meter> meters = new HashMap<>();
    final Map<String, Histogram> histograms = new HashMap<>();
    final Map<String, RecordingMetricGroup> subgroupsByName = new HashMap<>();

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

    @Override
    public <M extends Meter> M meter(String name, M meter) {
      M r = super.meter(name, meter);
      meters.put(name, r);
      return r;
    }

    @Override
    public <H extends Histogram> H histogram(String name, H histogram) {
      H h = super.histogram(name, histogram);
      histograms.put(name, h);
      return h;
    }

    @Override
    public MetricGroup addGroup(String name) {
      RecordingMetricGroup child = new RecordingMetricGroup();
      subgroupsByName.put(name, child);
      return child;
    }
  }
}
