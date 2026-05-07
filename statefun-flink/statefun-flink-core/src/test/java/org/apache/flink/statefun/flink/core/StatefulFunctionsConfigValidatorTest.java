// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.statefun.flink.core.exceptions.StatefulFunctionsInvalidConfigException;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.junit.jupiter.api.Test;

class StatefulFunctionsConfigValidatorTest {

  @Test
  void validatePassesForEmbeddedWithDefaults() {
    Configuration cfg = baseValidConfig();

    // isEmbedded=true skips the parent-first-classloader check entirely.
    assertThatCode(() -> StatefulFunctionsConfigValidator.validate(true, cfg))
        .doesNotThrowAnyException();
  }

  @Test
  void validateRejectsClassloaderPatternsMissingStateFunPrefix() {
    Configuration cfg = baseValidConfig();
    // Wipe out the parent-first list -> missing all 3 required patterns.
    cfg.set(
        CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL, Collections.singletonList("foo"));

    assertThatThrownBy(() -> StatefulFunctionsConfigValidator.validate(false, cfg))
        .isInstanceOf(StatefulFunctionsInvalidConfigException.class)
        .hasMessageContaining("org.apache.flink.statefun");
  }

  @Test
  void validatePassesWhenAllRequiredClassloaderPatternsPresentInAnyOrder() {
    Configuration cfg = baseValidConfig();
    cfg.set(
        CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
        Arrays.asList("com.google.protobuf", "org.apache.kafka", "org.apache.flink.statefun"));

    assertThatCode(() -> StatefulFunctionsConfigValidator.validate(false, cfg))
        .doesNotThrowAnyException();
  }

  @Test
  void validateRejectsCustomPayloadFactoryWithoutClass() {
    Configuration cfg = baseValidConfig();
    cfg.set(
        StatefulFunctionsConfig.USER_MESSAGE_SERIALIZER, MessageFactoryType.WITH_CUSTOM_PAYLOADS);
    // The class is left unset -> should fail.

    assertThatThrownBy(() -> StatefulFunctionsConfigValidator.validate(true, cfg))
        .isInstanceOf(StatefulFunctionsInvalidConfigException.class)
        .hasMessageContaining("custom payload serializer class must be supplied");
  }

  @Test
  void validateRejectsCustomPayloadClassWithoutMatchingFactory() {
    Configuration cfg = baseValidConfig();
    // Default factory is WITH_PROTOBUF_PAYLOADS but class is set anyway.
    cfg.set(
        StatefulFunctionsConfig.USER_MESSAGE_CUSTOM_PAYLOAD_SERIALIZER_CLASS, "com.foo.Bar");

    assertThatThrownBy(() -> StatefulFunctionsConfigValidator.validate(true, cfg))
        .isInstanceOf(StatefulFunctionsInvalidConfigException.class)
        .hasMessageContaining("may only be supplied with WITH_CUSTOM_PAYLOADS");
  }

  @Test
  void validatePassesWhenCustomFactoryAndClassBothSet() {
    Configuration cfg = baseValidConfig();
    cfg.set(
        StatefulFunctionsConfig.USER_MESSAGE_SERIALIZER, MessageFactoryType.WITH_CUSTOM_PAYLOADS);
    cfg.set(
        StatefulFunctionsConfig.USER_MESSAGE_CUSTOM_PAYLOAD_SERIALIZER_CLASS, "com.foo.Bar");

    assertThatCode(() -> StatefulFunctionsConfigValidator.validate(true, cfg))
        .doesNotThrowAnyException();
  }

  @Test
  void validateRejectsHeapBackedTimers() {
    Configuration cfg = baseValidConfig();
    cfg.setString("state.backend.rocksdb.timer-service.factory", "heap");

    assertThatThrownBy(() -> StatefulFunctionsConfigValidator.validate(true, cfg))
        .isInstanceOf(StatefulFunctionsInvalidConfigException.class)
        .hasMessageContaining("non-heap timers with a rocksdb state backend");
  }

  @Test
  void validateRejectsUnalignedCheckpointing() {
    Configuration cfg = baseValidConfig();
    cfg.setString("execution.checkpointing.unaligned", "true");

    assertThatThrownBy(() -> StatefulFunctionsConfigValidator.validate(true, cfg))
        .isInstanceOf(StatefulFunctionsInvalidConfigException.class)
        .hasMessageContaining("unaligned checkpointing");
  }

  private static Configuration baseValidConfig() {
    Configuration cfg = new Configuration();
    cfg.set(
        CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
        StatefulFunctionsConfigValidator.PARENT_FIRST_CLASSLOADER_PATTERNS);
    return cfg;
  }
}
