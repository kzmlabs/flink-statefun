// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.harness.io;

import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.IngressType;

@SuppressWarnings("WeakerAccess")
public class HarnessConstants {

  public static final IngressType SUPPLYING_INGRESS_TYPE =
      new IngressType("org.apache.flink.statefun.flink.harness", "supplier");
  public static final EgressType CONSUMING_EGRESS_TYPE =
      new EgressType("org.apache.flink.statefun.flink.harness", "consuming-egress");
}
