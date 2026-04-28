// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.e2e.smoke.driver;

import static org.apache.flink.statefun.e2e.smoke.driver.Types.unpackSourceCommand;

import java.util.Objects;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public class CommandRouter implements Router<TypedValue> {
  private final Ids ids;

  public CommandRouter(Ids ids) {
    this.ids = Objects.requireNonNull(ids);
  }

  @Override
  public void route(TypedValue command, Downstream<TypedValue> downstream) {
    SourceCommand sourceCommand = unpackSourceCommand(command);
    FunctionType type = Constants.FN_TYPE;
    String id = ids.idOf(sourceCommand.getTarget());
    downstream.forward(type, id, command);
  }
}
