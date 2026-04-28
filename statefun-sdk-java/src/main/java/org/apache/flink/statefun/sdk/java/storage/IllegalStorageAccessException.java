// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.sdk.java.storage;

public final class IllegalStorageAccessException extends RuntimeException {

  private static final long serialVersionUID = 1;

  protected IllegalStorageAccessException(String stateName, String message) {
    super("Error accessing state " + stateName + "; " + message);
  }
}
