// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.extensions;

import java.util.Map;
import org.apache.flink.statefun.sdk.TypeName;

/**
 * A module that binds multiple extension objects to the Stateful Functions application. Each
 * extension is uniquely identified by a {@link TypeName}.
 */
public interface ExtensionModule {

  /**
   * This method binds multiple extension objects to the Stateful Functions application.
   *
   * @param globalConfigurations global configuration of the Stateful Functions application.
   * @param binder binder for binding extensions.
   */
  void configure(Map<String, String> globalConfigurations, Binder binder);

  interface Binder {
    <T> void bindExtension(TypeName typeName, T extension);
  }
}
