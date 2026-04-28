// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.nettyclient.exceptions;

public final class WrongHttpResponse extends RuntimeException {

  public WrongHttpResponse(String message) {
    super(message);
  }
}
