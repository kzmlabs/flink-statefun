// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Optional;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.statefun.flink.core.exceptions.StatefulFunctionsInvalidConfigException;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

public class StatefulFunctionsConfigTest {

  private final String serializerClassName = "com.sample.Serializer";

  @Test
  public void testSetConfigurations() {
    final String testName = "test-name";

    Configuration configuration = new Configuration();
    configuration.set(StatefulFunctionsConfig.FLINK_JOB_NAME, testName);
    configuration.set(
        StatefulFunctionsConfig.USER_MESSAGE_SERIALIZER, MessageFactoryType.WITH_CUSTOM_PAYLOADS);
    configuration.set(
        StatefulFunctionsConfig.USER_MESSAGE_CUSTOM_PAYLOAD_SERIALIZER_CLASS, serializerClassName);
    configuration.set(
        StatefulFunctionsConfig.TOTAL_MEMORY_USED_FOR_FEEDBACK_CHECKPOINTING,
        MemorySize.ofMebiBytes(100));
    configuration.set(StatefulFunctionsConfig.ASYNC_MAX_OPERATIONS_PER_TASK, 100);
    configuration.set(
        CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
        Arrays.asList("org.apache.flink.statefun", "org.apache.kafka", "com.google.protobuf"));
    configuration.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, 1);
    configuration.setString("statefun.module.global-config.key1", "value1");
    configuration.setString("statefun.module.global-config.key2", "value2");

    StatefulFunctionsConfig stateFunConfig =
        StatefulFunctionsConfig.fromFlinkConfiguration(configuration);

    assertEquals(stateFunConfig.getFlinkJobName(), testName);
    assertEquals(stateFunConfig.getFactoryKey().getType(), MessageFactoryType.WITH_CUSTOM_PAYLOADS);
    assertEquals(
        stateFunConfig.getFactoryKey().getCustomPayloadSerializerClassName(),
        Optional.of(serializerClassName));
    assertEquals(stateFunConfig.getFeedbackBufferSize(), MemorySize.ofMebiBytes(100));
    assertEquals(stateFunConfig.getMaxAsyncOperationsPerTask(), 100);
    assertThat(stateFunConfig.getGlobalConfigurations(), Matchers.hasEntry("key1", "value1"));
    assertThat(stateFunConfig.getGlobalConfigurations(), Matchers.hasEntry("key2", "value2"));
  }

  private static Configuration baseConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set(StatefulFunctionsConfig.FLINK_JOB_NAME, "name");
    configuration.set(
        StatefulFunctionsConfig.USER_MESSAGE_SERIALIZER, MessageFactoryType.WITH_KRYO_PAYLOADS);
    configuration.set(
        StatefulFunctionsConfig.TOTAL_MEMORY_USED_FOR_FEEDBACK_CHECKPOINTING,
        MemorySize.ofMebiBytes(100));
    configuration.set(StatefulFunctionsConfig.ASYNC_MAX_OPERATIONS_PER_TASK, 100);
    configuration.set(
        CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
        Arrays.asList("org.apache.flink.statefun", "org.apache.kafka", "com.google.protobuf"));
    configuration.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, 1);
    return configuration;
  }

  @Test
  public void invalidCustomSerializerThrows() {
    assertThrows(
        StatefulFunctionsInvalidConfigException.class,
        () -> {
          Configuration configuration = baseConfiguration();
          configuration.set(
              StatefulFunctionsConfig.USER_MESSAGE_SERIALIZER,
              MessageFactoryType.WITH_CUSTOM_PAYLOADS);
          StatefulFunctionsConfigValidator.validate(false, configuration);
        });
  }

  @Test
  public void invalidNonCustomSerializerThrows() {
    assertThrows(
        StatefulFunctionsInvalidConfigException.class,
        () -> {
          Configuration configuration = baseConfiguration();
          configuration.set(
              StatefulFunctionsConfig.USER_MESSAGE_SERIALIZER,
              MessageFactoryType.WITH_KRYO_PAYLOADS);
          configuration.set(
              StatefulFunctionsConfig.USER_MESSAGE_CUSTOM_PAYLOAD_SERIALIZER_CLASS,
              serializerClassName);
          StatefulFunctionsConfigValidator.validate(false, configuration);
        });
  }
}
