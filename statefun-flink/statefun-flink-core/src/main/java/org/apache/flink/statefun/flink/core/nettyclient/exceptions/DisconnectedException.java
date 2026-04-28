// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.nettyclient.exceptions;

import java.io.IOException;

public final class DisconnectedException extends IOException {
  public static final DisconnectedException INSTANCE = new DisconnectedException();

  private DisconnectedException() {
    super("Disconnected");
    setStackTrace(new StackTraceElement[0]);
  }
}
