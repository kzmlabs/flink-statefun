// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.e2e.smoke.embedded;

import java.util.Objects;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public class CommandInterpreterFn implements StatefulFunction {

  @Persisted private final PersistedValue<Long> STATE = PersistedValue.of("state", Long.class);
  private final CommandInterpreter interpreter;

  public CommandInterpreterFn(CommandInterpreter interpreter) {
    this.interpreter = Objects.requireNonNull(interpreter);
  }

  @Override
  public void invoke(Context context, Object message) {
    interpreter.interpret(STATE, context, message);
  }
}
