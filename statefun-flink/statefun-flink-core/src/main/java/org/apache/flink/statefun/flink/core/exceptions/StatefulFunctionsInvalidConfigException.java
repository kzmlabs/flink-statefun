// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.exceptions;

import org.apache.flink.configuration.ConfigOption;

public final class StatefulFunctionsInvalidConfigException extends IllegalArgumentException {

  private static final long serialVersionUID = 1L;

  public StatefulFunctionsInvalidConfigException(ConfigOption<?> invalidConfig, String message) {
    super(String.format("Invalid configuration: %s; %s", invalidConfig.key(), message));
  }
}
