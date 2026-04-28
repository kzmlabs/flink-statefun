// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.kafka;

import java.time.Duration;
import java.util.Objects;

public abstract class KafkaProducerSemantic {

  public static KafkaProducerSemantic exactlyOnce(Duration transactionTimeout) {
    return new ExactlyOnce(transactionTimeout);
  }

  public static KafkaProducerSemantic atLeastOnce() {
    return new AtLeastOnce();
  }

  public static KafkaProducerSemantic none() {
    return new NoSemantics();
  }

  public boolean isExactlyOnceSemantic() {
    return getClass() == ExactlyOnce.class;
  }

  public ExactlyOnce asExactlyOnceSemantic() {
    return (ExactlyOnce) this;
  }

  public boolean isAtLeastOnceSemantic() {
    return getClass() == AtLeastOnce.class;
  }

  public AtLeastOnce asAtLeastOnceSemantic() {
    return (AtLeastOnce) this;
  }

  public boolean isNoSemantic() {
    return getClass() == NoSemantics.class;
  }

  public NoSemantics asNoSemantic() {
    return (NoSemantics) this;
  }

  public static class ExactlyOnce extends KafkaProducerSemantic {
    private final Duration transactionTimeout;

    private ExactlyOnce(Duration transactionTimeout) {
      if (transactionTimeout == Duration.ZERO) {
        throw new IllegalArgumentException(
            "Transaction timeout durations must be larger than 0 when using exactly-once producer semantics.");
      }
      this.transactionTimeout = Objects.requireNonNull(transactionTimeout);
    }

    public Duration transactionTimeout() {
      return transactionTimeout;
    }
  }

  public static class AtLeastOnce extends KafkaProducerSemantic {
    private AtLeastOnce() {}
  }

  public static class NoSemantics extends KafkaProducerSemantic {
    private NoSemantics() {}
  }
}
