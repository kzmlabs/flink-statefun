// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.metrics;

import org.apache.flink.statefun.sdk.FunctionType;

public interface FuncionTypeMetricsFactory {

  FunctionTypeMetrics forType(FunctionType functionType);
}
