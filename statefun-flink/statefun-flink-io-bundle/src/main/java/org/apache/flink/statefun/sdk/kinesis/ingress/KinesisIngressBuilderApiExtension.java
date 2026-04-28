// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.sdk.kinesis.ingress;

public class KinesisIngressBuilderApiExtension {
  public static <T> void withDeserializer(
      KinesisIngressBuilder<T> kinesisIngressBuilder, KinesisIngressDeserializer<T> deserializer) {
    kinesisIngressBuilder.withDeserializer(deserializer);
  }
}
