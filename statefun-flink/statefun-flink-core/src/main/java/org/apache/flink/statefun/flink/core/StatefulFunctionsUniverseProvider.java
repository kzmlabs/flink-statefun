// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core;

import java.io.Serializable;

public interface StatefulFunctionsUniverseProvider extends Serializable {

  StatefulFunctionsUniverse get(ClassLoader classLoader, StatefulFunctionsConfig configuration);
}
