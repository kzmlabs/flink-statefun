// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kinesis;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.flink.io.spi.FlinkIoModule;
import org.apache.flink.statefun.sdk.kinesis.KinesisIOTypes;

@AutoService(FlinkIoModule.class)
public final class KinesisFlinkIoModule implements FlinkIoModule {

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    binder.bindSourceProvider(KinesisIOTypes.UNIVERSAL_INGRESS_TYPE, new KinesisSourceProvider());
    binder.bindSinkProvider(KinesisIOTypes.UNIVERSAL_EGRESS_TYPE, new KinesisSinkProvider());
  }
}
