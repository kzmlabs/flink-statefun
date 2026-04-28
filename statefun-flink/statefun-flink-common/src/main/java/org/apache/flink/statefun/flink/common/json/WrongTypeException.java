// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.common.json;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;

public class WrongTypeException extends RuntimeException {

  private static final long serialVersionUID = 1;

  public WrongTypeException(JsonPointer pointer, String message) {
    super("Wrong type for key " + pointer + " " + message);
  }
}
