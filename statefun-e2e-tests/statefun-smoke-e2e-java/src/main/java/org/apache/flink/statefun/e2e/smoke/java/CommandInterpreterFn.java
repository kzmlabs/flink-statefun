// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.e2e.smoke.java;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.Message;

public class CommandInterpreterFn implements StatefulFunction {

  public static final ValueSpec<Long> STATE = ValueSpec.named("state").withLongType();
  private final CommandInterpreter interpreter;

  public CommandInterpreterFn(CommandInterpreter interpreter) {
    this.interpreter = interpreter;
  }

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
    interpreter.interpret(STATE, context, message);
    return context.done();
  }
}
