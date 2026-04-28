// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.metrics;

import java.util.Objects;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.metrics.Metrics;

public class FlinkFuncionTypeMetricsFactory implements FuncionTypeMetricsFactory {

  private final MetricGroup metricGroup;

  public FlinkFuncionTypeMetricsFactory(MetricGroup metricGroup) {
    this.metricGroup = Objects.requireNonNull(metricGroup);
  }

  @Override
  public FunctionTypeMetrics forType(FunctionType functionType) {
    MetricGroup namespace = metricGroup.addGroup(functionType.namespace());
    MetricGroup typeGroup = namespace.addGroup(functionType.name());
    Metrics functionTypeScopedMetrics = new FlinkUserMetrics(typeGroup);
    return new FlinkFunctionTypeMetrics(typeGroup, functionTypeScopedMetrics);
  }
}
