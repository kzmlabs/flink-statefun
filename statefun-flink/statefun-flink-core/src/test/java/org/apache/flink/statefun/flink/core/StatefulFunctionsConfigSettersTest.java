// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

/**
 * Covers the {@link StatefulFunctionsConfig} setter / accessor surface that
 * StatefulFunctionsConfigTest doesn't reach: setFactoryType / setEmbedded / setProvider /
 * getRemoteModuleName / addAllGlobalConfigurations / fromEnvironment.
 */
class StatefulFunctionsConfigSettersTest {

  @Test
  void fromFlinkConfigurationWithDefaultsReturnsDocumentedDefaults() {
    StatefulFunctionsConfig cfg =
        StatefulFunctionsConfig.fromFlinkConfiguration(new Configuration());

    assertThat(cfg.getFlinkJobName()).isEqualTo("StatefulFunctions");
    assertThat(cfg.getFactoryType()).isEqualTo(MessageFactoryType.WITH_PROTOBUF_PAYLOADS);
    assertThat(cfg.getFeedbackBufferSize()).isEqualTo(MemorySize.ofMebiBytes(32));
    assertThat(cfg.getMaxAsyncOperationsPerTask()).isEqualTo(32 * 1024);
    assertThat(cfg.getRemoteModuleName()).isEqualTo("classpath:module.yaml");
    assertThat(cfg.isEmbedded()).isFalse();
    assertThat(cfg.getCustomPayloadSerializerClassName()).isNull();
    assertThat(cfg.getGlobalConfigurations()).isEmpty();
  }

  @Test
  void fromEnvironmentReadsFromStreamExecutionEnvironmentConfiguration() {
    Configuration sentinel = new Configuration();
    sentinel.set(StatefulFunctionsConfig.FLINK_JOB_NAME, "from-env-job");
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(1, sentinel);

    StatefulFunctionsConfig cfg = StatefulFunctionsConfig.fromEnvironment(env);

    assertThat(cfg.getFlinkJobName()).isEqualTo("from-env-job");
  }

  @Test
  void settersOverrideValuesAfterFromFlinkConfiguration() {
    StatefulFunctionsConfig cfg =
        StatefulFunctionsConfig.fromFlinkConfiguration(new Configuration());

    cfg.setFlinkJobName("renamed");
    cfg.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);
    cfg.setCustomPayloadSerializerClassName("com.foo.Serializer");
    cfg.setFeedbackBufferSize(MemorySize.ofMebiBytes(64));
    cfg.setMaxAsyncOperationsPerTask(2048);
    cfg.setRemoteModuleName("file:///tmp/module.yaml");
    cfg.setEmbedded(true);

    assertThat(cfg.getFlinkJobName()).isEqualTo("renamed");
    assertThat(cfg.getFactoryType()).isEqualTo(MessageFactoryType.WITH_KRYO_PAYLOADS);
    assertThat(cfg.getCustomPayloadSerializerClassName()).isEqualTo("com.foo.Serializer");
    assertThat(cfg.getFeedbackBufferSize()).isEqualTo(MemorySize.ofMebiBytes(64));
    assertThat(cfg.getMaxAsyncOperationsPerTask()).isEqualTo(2048);
    assertThat(cfg.getRemoteModuleName()).isEqualTo("file:///tmp/module.yaml");
    assertThat(cfg.isEmbedded()).isTrue();
  }

  @Test
  void settersRejectNullForRequiredFields() {
    StatefulFunctionsConfig cfg =
        StatefulFunctionsConfig.fromFlinkConfiguration(new Configuration());

    assertThatThrownBy(() -> cfg.setFactoryType(null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> cfg.setFlinkJobName(null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> cfg.setFeedbackBufferSize(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> cfg.setRemoteModuleName(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void getFactoryKeyReflectsCurrentTypeAndCustomSerializerName() {
    StatefulFunctionsConfig cfg =
        StatefulFunctionsConfig.fromFlinkConfiguration(new Configuration());
    cfg.setFactoryType(MessageFactoryType.WITH_CUSTOM_PAYLOADS);
    cfg.setCustomPayloadSerializerClassName("com.foo.Bar");

    assertThat(cfg.getFactoryKey().getType()).isEqualTo(MessageFactoryType.WITH_CUSTOM_PAYLOADS);
    assertThat(cfg.getFactoryKey().getCustomPayloadSerializerClassName()).hasValue("com.foo.Bar");
  }

  @Test
  void addAllGlobalConfigurationsMergesIntoExistingMap() {
    Configuration cfg = new Configuration();
    cfg.setString("statefun.module.global-config.preexisting", "from-flink-conf");
    StatefulFunctionsConfig stateFunCfg = StatefulFunctionsConfig.fromFlinkConfiguration(cfg);

    Map<String, String> additions = new HashMap<>();
    additions.put("added", "from-runtime");
    additions.put("preexisting", "overwritten");
    stateFunCfg.addAllGlobalConfigurations(additions);

    Map<String, String> resolved = stateFunCfg.getGlobalConfigurations();
    assertThat(resolved).containsEntry("added", "from-runtime");
    // addAllGlobalConfigurations uses putAll — pinned to overwrite-on-conflict semantics.
    assertThat(resolved).containsEntry("preexisting", "overwritten");
  }

  @Test
  void getGlobalConfigurationsIsImmutable() {
    StatefulFunctionsConfig cfg =
        StatefulFunctionsConfig.fromFlinkConfiguration(new Configuration());

    assertThatThrownBy(() -> cfg.getGlobalConfigurations().put("k", "v"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void universeProviderRoundtripsThroughByteSerialization() {
    StatefulFunctionsConfig cfg =
        StatefulFunctionsConfig.fromFlinkConfiguration(new Configuration());

    SerializableProvider provider = new SerializableProvider("payload");
    cfg.setProvider(provider);
    StatefulFunctionsUniverseProvider restored = cfg.getProvider(getClass().getClassLoader());

    assertThat(restored).isInstanceOf(SerializableProvider.class);
    assertThat(((SerializableProvider) restored).payload).isEqualTo("payload");
  }

  @Test
  void getProviderBeforeSetThrows() {
    StatefulFunctionsConfig cfg =
        StatefulFunctionsConfig.fromFlinkConfiguration(new Configuration());

    // Pin: calling getProvider before setProvider is misuse. Implementation deserializes a
    // null byte array which surfaces as NPE through Flink's InstantiationUtil — either NPE or
    // IllegalStateException is a loud failure, but it must not silently return null.
    assertThatThrownBy(() -> cfg.getProvider(getClass().getClassLoader()))
        .isInstanceOf(RuntimeException.class);
  }

  /** Minimal StatefulFunctionsUniverseProvider that survives serialize → deserialize. */
  private static final class SerializableProvider
      implements StatefulFunctionsUniverseProvider, Serializable {
    private static final long serialVersionUID = 1L;
    final String payload;

    SerializableProvider(String payload) {
      this.payload = payload;
    }

    @Override
    public StatefulFunctionsUniverse get(
        ClassLoader classLoader, StatefulFunctionsConfig configuration) {
      throw new UnsupportedOperationException("Test double — never invoked.");
    }

    // Used implicitly by the contract — keep here so reflection-based discovery works.
    @SuppressWarnings("unused")
    static final Class<? extends StatefulFunctionModule> MODULE_INTERFACE_REF =
        StatefulFunctionModule.class;
  }
}
