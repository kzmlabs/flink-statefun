// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.e2e.smoke.driver;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public class Constants {

  public static final String NAMESPACE = "statefun.smoke.e2e";
  public static final String INGRESS_NAME = "command-generator-source";
  public static final String EGRESS_NAME = "discard-sink";
  public static final String VERIFICATION_EGRESS_NAME = "verification-sink";
  public static final String FUNCTION_NAME = "command-interpreter-fn";

  public static final IngressIdentifier<TypedValue> IN =
      new IngressIdentifier<>(TypedValue.class, NAMESPACE, INGRESS_NAME);

  public static final EgressIdentifier<TypedValue> OUT =
      new EgressIdentifier<>(NAMESPACE, EGRESS_NAME, TypedValue.class);

  public static final EgressIdentifier<TypedValue> VERIFICATION_RESULT =
      new EgressIdentifier<>(NAMESPACE, VERIFICATION_EGRESS_NAME, TypedValue.class);

  // For embedded/remote functions to bind with the smoke-e2e-common testing framework
  public static final FunctionType FN_TYPE = new FunctionType(NAMESPACE, FUNCTION_NAME);
}
