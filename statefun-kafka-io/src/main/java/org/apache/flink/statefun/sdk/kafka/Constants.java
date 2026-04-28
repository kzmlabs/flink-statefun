// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.kafka;

import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.IngressType;

public final class Constants {
  public static final IngressType KAFKA_INGRESS_TYPE =
      new IngressType("statefun.kafka.io", "universal-ingress");
  public static final EgressType KAFKA_EGRESS_TYPE =
      new EgressType("statefun.kafka.io", "universal-egress");

  private Constants() {}
}
