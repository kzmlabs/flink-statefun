// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.io.kafka.binders.egress.v1;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.extensions.ExtensionModule;

@AutoService(ExtensionModule.class)
public final class Module implements ExtensionModule {

  @Override
  public void configure(Map<String, String> globalConfigurations, Binder universeBinder) {
    universeBinder.bindExtension(
        GenericKafkaEgressBinderV1.KIND_TYPE, GenericKafkaEgressBinderV1.INSTANCE);
  }
}
