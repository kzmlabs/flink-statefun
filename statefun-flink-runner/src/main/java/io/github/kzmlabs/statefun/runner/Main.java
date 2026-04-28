// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package io.github.kzmlabs.statefun.runner;

import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsJob;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverseProvider;
import org.apache.flink.statefun.flink.core.spi.Modules;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    StatefulFunctionsConfig stateFunConfig = StatefulFunctionsConfig.fromEnvironment(env);

    stateFunConfig.setProvider(
        (StatefulFunctionsUniverseProvider)
            (classLoader, statefulFunctionsConfig) -> {
              Modules modules = Modules.loadFromClassPath(statefulFunctionsConfig);
              return modules.createStatefulFunctionsUniverse();
            });

    StatefulFunctionsJob.main(env, stateFunConfig);
  }
}
