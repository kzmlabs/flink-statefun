// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core;

import org.apache.flink.statefun.flink.core.spi.Modules;
import org.apache.flink.util.Preconditions;

public final class StatefulFunctionsUniverses {

  public static StatefulFunctionsUniverse get(
      ClassLoader classLoader, StatefulFunctionsConfig configuration) {
    Preconditions.checkState(classLoader != null, "The class loader was not set.");
    Preconditions.checkState(configuration != null, "The configuration was not set.");

    StatefulFunctionsUniverseProvider provider = configuration.getProvider(classLoader);

    return provider.get(classLoader, configuration);
  }

  static final class ClassPathUniverseProvider implements StatefulFunctionsUniverseProvider {

    private static final long serialVersionUID = 1;

    @Override
    public StatefulFunctionsUniverse get(
        ClassLoader classLoader, StatefulFunctionsConfig configuration) {
      Modules modules = Modules.loadFromClassPath(configuration);
      return modules.createStatefulFunctionsUniverse();
    }
  }
}
