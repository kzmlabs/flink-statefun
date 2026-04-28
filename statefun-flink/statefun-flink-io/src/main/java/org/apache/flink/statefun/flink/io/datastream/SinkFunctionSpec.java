// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.datastream;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;

/**
 * An {@link EgressSpec} that can run any Apache Flink {@link Sink}.
 *
 * @param <T> The input type output by the sink.
 */
public final class SinkFunctionSpec<T> implements EgressSpec<T>, Serializable {
  private static final long serialVersionUID = 1;

  static final EgressType TYPE =
      new EgressType("org.apache.flink.statefun.flink.io", "sink-function-spec");

  private final EgressIdentifier<T> id;
  private final Sink<T> delegate;

  /**
   * @param id A unique egress identifier.
   * @param delegate The underlying sink that the egress will delegate to at runtime.
   */
  public SinkFunctionSpec(EgressIdentifier<T> id, Sink<T> delegate) {
    this.id = Objects.requireNonNull(id);
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public final EgressIdentifier<T> id() {
    return id;
  }

  @Override
  public final EgressType type() {
    return TYPE;
  }

  Sink<T> delegate() {
    return delegate;
  }
}
