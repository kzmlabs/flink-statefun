// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.metrics;

import static org.apache.flink.statefun.flink.core.metrics.FlinkMetricUtil.wrapFlinkCounterAsSdkCounter;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashMap;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.statefun.sdk.metrics.Counter;
import org.apache.flink.statefun.sdk.metrics.Metrics;

@Internal
public final class FlinkUserMetrics implements Metrics {
  private final ObjectOpenHashMap<String, Counter> counters = new ObjectOpenHashMap<>();
  private final MetricGroup typeGroup;

  public FlinkUserMetrics(MetricGroup typeGroup) {
    this.typeGroup = Objects.requireNonNull(typeGroup);
  }

  @Override
  public Counter counter(String name) {
    Objects.requireNonNull(name);
    Counter counter = counters.get(name);
    if (counter == null) {
      SimpleCounter internalCounter = typeGroup.counter(name, new SimpleCounter());
      counters.put(name, counter = wrapFlinkCounterAsSdkCounter(internalCounter));
    }
    return counter;
  }
}
