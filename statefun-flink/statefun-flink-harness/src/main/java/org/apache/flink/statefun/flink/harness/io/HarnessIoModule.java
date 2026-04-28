// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.harness.io;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.statefun.flink.io.spi.FlinkIoModule;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.io.IngressSpec;

@AutoService(FlinkIoModule.class)
public class HarnessIoModule implements FlinkIoModule {

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    binder.bindSourceProvider(
        HarnessConstants.SUPPLYING_INGRESS_TYPE, HarnessIoModule::supplingIngressSpec);
    binder.bindSinkProvider(
        HarnessConstants.CONSUMING_EGRESS_TYPE, HarnessIoModule::consumingEgressSpec);
  }

  @SuppressWarnings("unchecked")
  private static <T> Source supplingIngressSpec(IngressSpec<T> spec) {
    SupplyingIngressSpec<T> casted = (SupplyingIngressSpec<T>) spec;
    return new SupplyingSource<>(casted.supplier());
  }

  private static <T> Sink<T> consumingEgressSpec(EgressSpec<T> spec) {
    if (!(spec instanceof ConsumingEgressSpec)) {
      throw new IllegalArgumentException("Unable to provider a source for " + spec);
    }
    ConsumingEgressSpec<T> casted = (ConsumingEgressSpec<T>) spec;
    return new ConsumingSink<>(casted.consumer());
  }
}
