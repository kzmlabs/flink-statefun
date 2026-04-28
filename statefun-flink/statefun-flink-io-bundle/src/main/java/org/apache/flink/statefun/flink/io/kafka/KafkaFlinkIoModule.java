// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.io.kafka;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.flink.io.spi.FlinkIoModule;
import org.apache.flink.statefun.sdk.kafka.Constants;

@AutoService(FlinkIoModule.class)
public final class KafkaFlinkIoModule implements FlinkIoModule {

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    binder.bindSourceProvider(Constants.KAFKA_INGRESS_TYPE, new KafkaSourceProvider());
    binder.bindSinkProvider(Constants.KAFKA_EGRESS_TYPE, new KafkaSinkProvider());
  }
}
