// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.types.remote;

import org.apache.flink.statefun.sdk.TypeName;

public final class RemoteValueTypeMismatchException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  public RemoteValueTypeMismatchException(TypeName previousValueType, TypeName newValueType) {
    super(String.format("Previous type was: %s, new type is: %s", previousValueType, newValueType));
  }
}
