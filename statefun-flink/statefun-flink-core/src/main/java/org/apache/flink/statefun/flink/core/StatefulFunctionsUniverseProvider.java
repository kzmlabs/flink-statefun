// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core;

import java.io.Serializable;

public interface StatefulFunctionsUniverseProvider extends Serializable {

  StatefulFunctionsUniverse get(ClassLoader classLoader, StatefulFunctionsConfig configuration);
}
