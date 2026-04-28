// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.nettyclient.exceptions;

public class NoMoreRoutesException extends RuntimeException {
  public NoMoreRoutesException(String message) {
    super(message);
  }
}
