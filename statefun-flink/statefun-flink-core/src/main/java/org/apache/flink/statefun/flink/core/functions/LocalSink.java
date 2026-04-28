// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.functions;

import java.util.Objects;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.di.Lazy;
import org.apache.flink.statefun.flink.core.message.Message;

final class LocalSink {
  private final Lazy<LocalFunctionGroup> functionGroup;

  @Inject
  LocalSink(@Label("function-group") Lazy<LocalFunctionGroup> functionGroup) {
    this.functionGroup = Objects.requireNonNull(functionGroup);
  }

  void accept(Message message) {
    Objects.requireNonNull(message);
    functionGroup.get().enqueue(message);
  }
}
