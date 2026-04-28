// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.kinesis;

import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.IngressType;

public final class KinesisIOTypes {

  private KinesisIOTypes() {}

  public static final IngressType UNIVERSAL_INGRESS_TYPE =
      new IngressType("statefun.kinesis.io", "universal-ingress");
  public static final EgressType UNIVERSAL_EGRESS_TYPE =
      new EgressType("statefun.kinesis.io", "universal-egress");
}
