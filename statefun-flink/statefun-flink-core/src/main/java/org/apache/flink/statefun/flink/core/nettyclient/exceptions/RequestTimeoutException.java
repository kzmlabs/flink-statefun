// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.nettyclient.exceptions;

import java.util.concurrent.TimeoutException;

public final class RequestTimeoutException extends TimeoutException {
  public static final RequestTimeoutException INSTANCE = new RequestTimeoutException();

  public RequestTimeoutException() {
    setStackTrace(new StackTraceElement[] {});
  }
}
