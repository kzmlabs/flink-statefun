// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.e2e.smoke.driver;

import org.apache.flink.statefun.e2e.smoke.generated.Commands;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.apache.flink.statefun.e2e.smoke.generated.VerificationResult;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class Types {
  private Types() {}

  public static final TypeName SOURCE_COMMANDS_TYPE =
      TypeName.parseFrom(Constants.NAMESPACE + "/source-command");
  public static final TypeName VERIFICATION_RESULT_TYPE =
      TypeName.parseFrom(Constants.NAMESPACE + "/verification-result");
  public static final TypeName COMMANDS_TYPE =
      TypeName.parseFrom(Constants.NAMESPACE + "/commands");

  public static boolean isTypeOf(TypedValue value, TypeName type) {
    return value.getTypename().equals(type.canonicalTypenameString());
  }

  public static TypedValue packSourceCommand(SourceCommand sourceCommand) {
    return TypedValue.newBuilder()
        .setTypename(SOURCE_COMMANDS_TYPE.canonicalTypenameString())
        .setHasValue(true)
        .setValue(sourceCommand.toByteString())
        .build();
  }

  public static SourceCommand unpackSourceCommand(TypedValue typedValue) {
    if (!isTypeOf(typedValue, SOURCE_COMMANDS_TYPE)) {
      throw new IllegalStateException("Unexpected TypedValue: " + typedValue);
    }
    try {
      return SourceCommand.parseFrom(typedValue.getValue());
    } catch (Exception e) {
      throw new RuntimeException("Unable to parse SourceCommand from TypedValue.", e);
    }
  }

  public static TypedValue packCommands(Commands commands) {
    return TypedValue.newBuilder()
        .setTypename(COMMANDS_TYPE.canonicalTypenameString())
        .setHasValue(true)
        .setValue(commands.toByteString())
        .build();
  }

  public static Commands unpackCommands(TypedValue typedValue) {
    if (!isTypeOf(typedValue, COMMANDS_TYPE)) {
      throw new IllegalStateException("Unexpected TypedValue: " + typedValue);
    }
    try {
      return Commands.parseFrom(typedValue.getValue());
    } catch (Exception e) {
      throw new RuntimeException("Unable to parse Commands from TypedValue.", e);
    }
  }

  public static TypedValue packVerificationResult(VerificationResult verificationResult) {
    return TypedValue.newBuilder()
        .setTypename(VERIFICATION_RESULT_TYPE.canonicalTypenameString())
        .setHasValue(true)
        .setValue(verificationResult.toByteString())
        .build();
  }

  public static VerificationResult unpackVerificationResult(TypedValue typedValue) {
    if (!isTypeOf(typedValue, VERIFICATION_RESULT_TYPE)) {
      throw new IllegalStateException("Unexpected TypedValue: " + typedValue);
    }
    try {
      return VerificationResult.parseFrom(typedValue.getValue());
    } catch (Exception e) {
      throw new RuntimeException("Unable to parse SourceCommand from TypedValue.", e);
    }
  }
}
