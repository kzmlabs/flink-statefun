// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.common;

import java.util.Objects;
import java.util.concurrent.Executor;
import org.apache.flink.api.common.operators.MailboxExecutor;

public final class MailboxExecutorFacade implements Executor {
  private final MailboxExecutor executor;
  private final String name;

  public MailboxExecutorFacade(MailboxExecutor executor, String name) {
    this.executor = Objects.requireNonNull(executor);
    this.name = Objects.requireNonNull(name);
  }

  @Override
  public void execute(Runnable command) {
    executor.execute(command::run, name);
  }
}
