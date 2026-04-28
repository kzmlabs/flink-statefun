// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core;

final class StatefulFunctionsUniverseValidator {

  void validate(StatefulFunctionsUniverse statefulFunctionsUniverse) {
    if (statefulFunctionsUniverse.ingress().isEmpty()) {
      throw new IllegalStateException("There are no ingress defined.");
    }
    if (statefulFunctionsUniverse.sources().isEmpty()) {
      throw new IllegalStateException("There are no source providers defined.");
    }
    if (statefulFunctionsUniverse.routers().isEmpty()) {
      throw new IllegalStateException("There are no routers defined.");
    }
    if (statefulFunctionsUniverse.functions().isEmpty()
        && statefulFunctionsUniverse.namespaceFunctions().isEmpty()) {
      throw new IllegalStateException("There are no function providers defined.");
    }
  }
}
