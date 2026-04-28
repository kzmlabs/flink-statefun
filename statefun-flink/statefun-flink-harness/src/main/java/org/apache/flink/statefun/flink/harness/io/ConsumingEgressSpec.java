// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.harness.io;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;

public final class ConsumingEgressSpec<T> implements EgressSpec<T>, Serializable {

  private static final long serialVersionUID = 1;

  private final EgressIdentifier<T> id;
  private final SerializableConsumer<T> consumer;

  public ConsumingEgressSpec(EgressIdentifier<T> id, SerializableConsumer<T> consumer) {
    this.id = Objects.requireNonNull(id);
    this.consumer = Objects.requireNonNull(consumer);
  }

  @Override
  public EgressIdentifier<T> id() {
    return id;
  }

  @Override
  public EgressType type() {
    return HarnessConstants.CONSUMING_EGRESS_TYPE;
  }

  SerializableConsumer<T> consumer() {
    return consumer;
  }
}
