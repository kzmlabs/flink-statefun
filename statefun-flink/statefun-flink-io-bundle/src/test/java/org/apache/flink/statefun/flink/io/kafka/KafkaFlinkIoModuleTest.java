// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.io.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.statefun.flink.io.spi.FlinkIoModule;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.kafka.Constants;
import org.junit.jupiter.api.Test;

class KafkaFlinkIoModuleTest {

  @Test
  void configureBindsBothSourceAndSinkProviders() {
    KafkaFlinkIoModule module = new KafkaFlinkIoModule();
    RecordingBinder binder = new RecordingBinder();

    module.configure(Collections.emptyMap(), binder);

    assertThat(binder.sourceBindings).hasSize(1);
    assertThat(binder.sourceBindings.get(Constants.KAFKA_INGRESS_TYPE))
        .isInstanceOf(KafkaSourceProvider.class);

    assertThat(binder.sinkBindings).hasSize(1);
    assertThat(binder.sinkBindings.get(Constants.KAFKA_EGRESS_TYPE))
        .isInstanceOf(KafkaSinkProvider.class);
  }

  private static final class RecordingBinder implements FlinkIoModule.Binder {
    final Map<IngressType, SourceProvider> sourceBindings = new HashMap<>();
    final Map<EgressType, SinkProvider> sinkBindings = new HashMap<>();

    @Override
    public void bindSourceProvider(IngressType type, SourceProvider provider) {
      sourceBindings.put(type, provider);
    }

    @Override
    public void bindSinkProvider(EgressType type, SinkProvider provider) {
      sinkBindings.put(type, provider);
    }
  }
}
