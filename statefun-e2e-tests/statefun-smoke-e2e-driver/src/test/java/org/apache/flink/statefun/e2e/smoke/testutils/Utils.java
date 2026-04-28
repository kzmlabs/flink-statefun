// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.e2e.smoke.testutils;

import org.apache.flink.statefun.e2e.smoke.generated.Command;
import org.apache.flink.statefun.e2e.smoke.generated.Commands;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;

public class Utils {

  public static SourceCommand aStateModificationCommand() {
    return aStateModificationCommand(-1234); // the id doesn't matter
  }

  public static SourceCommand aStateModificationCommand(int functionInstanceId) {
    return SourceCommand.newBuilder()
        .setTarget(functionInstanceId)
        .setCommands(Commands.newBuilder().addCommand(modify()))
        .build();
  }

  public static SourceCommand aRelayedStateModificationCommand(
      int firstFunctionId, int secondFunctionId) {
    return SourceCommand.newBuilder()
        .setTarget(firstFunctionId)
        .setCommands(Commands.newBuilder().addCommand(sendTo(secondFunctionId, modify())))
        .build();
  }

  private static Command.Builder sendTo(int id, Command.Builder body) {
    return Command.newBuilder()
        .setSend(
            Command.Send.newBuilder()
                .setTarget(id)
                .setCommands(Commands.newBuilder().addCommand(body)));
  }

  private static Command.Builder modify() {
    return Command.newBuilder().setIncrement(Command.IncrementState.getDefaultInstance());
  }
}
