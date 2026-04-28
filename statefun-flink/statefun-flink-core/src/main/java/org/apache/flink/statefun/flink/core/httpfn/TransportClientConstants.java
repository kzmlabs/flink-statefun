// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.httpfn;

import org.apache.flink.statefun.sdk.TypeName;

public final class TransportClientConstants {

  public static final TypeName OKHTTP_CLIENT_FACTORY_TYPE =
      TypeName.parseFrom("io.statefun.transports.v1/okhttp");

  public static final TypeName ASYNC_CLIENT_FACTORY_TYPE =
      TypeName.parseFrom("io.statefun.transports.v1/async");
}
