// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.kafka;

import java.util.Locale;

/** The auto offset reset position to use in case consumed offsets are invalid. */
public enum KafkaIngressAutoResetPosition {
  EARLIEST,
  LATEST;

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ENGLISH);
  }
}
