// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.jsonmodule;

public final class ModuleConfigurationException extends RuntimeException {
  private static final long serialVersionUID = 1;

  public ModuleConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }

  public ModuleConfigurationException(String message) {
    super(message);
  }
}
