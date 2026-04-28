// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.common;

import org.apache.flink.statefun.sdk.Address;

public final class KeyBy {
  private KeyBy() {}

  public static String apply(Address address) {
    return address.id();
  }
}
