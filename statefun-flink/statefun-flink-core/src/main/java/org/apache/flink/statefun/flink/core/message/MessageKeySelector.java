// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.message;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.statefun.flink.core.common.KeyBy;

public final class MessageKeySelector implements KeySelector<Message, String> {

  private static final long serialVersionUID = 1;

  @Override
  public String getKey(Message value) {
    return KeyBy.apply(value.target());
  }
}
