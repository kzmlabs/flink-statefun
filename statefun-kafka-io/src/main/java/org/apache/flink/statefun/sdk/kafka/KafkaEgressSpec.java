// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.kafka;

import java.util.Objects;
import java.util.Properties;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;

public final class KafkaEgressSpec<OutT> implements EgressSpec<OutT> {
  private final Class<? extends KafkaEgressSerializer<OutT>> serializer;
  private final String kafkaAddress;
  private final Properties properties;
  private final EgressIdentifier<OutT> id;
  private final int kafkaProducerPoolSize;
  private final KafkaProducerSemantic semantic;

  KafkaEgressSpec(
      EgressIdentifier<OutT> id,
      Class<? extends KafkaEgressSerializer<OutT>> serializer,
      String kafkaAddress,
      Properties properties,
      int kafkaProducerPoolSize,
      KafkaProducerSemantic semantic) {
    this.serializer = Objects.requireNonNull(serializer);
    this.kafkaAddress = Objects.requireNonNull(kafkaAddress);
    this.properties = Objects.requireNonNull(properties);
    this.id = Objects.requireNonNull(id);
    this.kafkaProducerPoolSize = kafkaProducerPoolSize;
    this.semantic = Objects.requireNonNull(semantic);
  }

  @Override
  public EgressIdentifier<OutT> id() {
    return id;
  }

  @Override
  public EgressType type() {
    return Constants.KAFKA_EGRESS_TYPE;
  }

  public Class<? extends KafkaEgressSerializer<OutT>> serializerClass() {
    return serializer;
  }

  public String kafkaAddress() {
    return kafkaAddress;
  }

  public Properties properties() {
    return properties;
  }

  public int kafkaProducerPoolSize() {
    return kafkaProducerPoolSize;
  }

  public KafkaProducerSemantic semantic() {
    return semantic;
  }
}
