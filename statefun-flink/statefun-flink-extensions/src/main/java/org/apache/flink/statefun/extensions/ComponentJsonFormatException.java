// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.extensions;

public final class ComponentJsonFormatException extends IllegalArgumentException {
  private static final long serialVersionUID = 1L;

  public ComponentJsonFormatException(String message) {
    super(message);
  }

  public ComponentJsonFormatException(String message, Throwable cause) {
    super(message, cause);
  }
}
