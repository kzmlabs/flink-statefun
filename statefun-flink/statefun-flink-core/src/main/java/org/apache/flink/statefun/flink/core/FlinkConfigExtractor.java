// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

final class FlinkConfigExtractor {

  /**
   * Reflectively extracts Flink {@link Configuration} from a {@link StreamExecutionEnvironment}.
   * The Flink configuration contains Stateful Functions specific configurations. This is currently
   * a private method in the {@code StreamExecutionEnvironment} class.
   */
  static Configuration reflectivelyExtractFromEnv(StreamExecutionEnvironment env) {
    try {
      return (Configuration) getConfigurationMethod().invoke(env);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(
          "Failed to acquire the Flink configuration from the current environment", e);
    }
  }

  private static Method getConfigurationMethod() throws NoSuchMethodException {
    Method getConfiguration =
        StreamExecutionEnvironment.class.getDeclaredMethod("getConfiguration");
    getConfiguration.setAccessible(true);
    return getConfiguration;
  }
}
