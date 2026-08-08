// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.io.testutils;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;

/**
 * Proxy-backed MetricGroup for tests: records every counter under its dotted scope (group
 * key-value pairs joined as key.value) so assertions can read counts by full name.
 */
public final class RecordingMetricGroup {

  private final Map<String, Counter> counters = new HashMap<>();

  public MetricGroup group() {
    return group("");
  }

  private MetricGroup group(String scope) {
    return (MetricGroup) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {MetricGroup.class}, (proxy, method, args) -> {
      if (method.getName().equals("counter") && args != null && args.length == 1) {
        return counters.computeIfAbsent(scope + args[0], k -> new SimpleCounter());
      }
      if (method.getName().equals("addGroup") && args != null && args.length == 2) {
        return group(scope + args[0] + "." + args[1] + ".");
      }
      return null;
    });
  }

  public long count(String name) {
    Counter counter = counters.get(name);
    return counter == null ? 0 : counter.getCount();
  }
}
