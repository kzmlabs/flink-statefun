// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.nettyclient.exceptions;

public final class ShutdownException extends RuntimeException {

  public static final ShutdownException INSTANCE = new ShutdownException();

  public ShutdownException() {
    super("Shutdown");
    setStackTrace(new StackTraceElement[] {});
  }
}
