// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.translation;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;

final class DecoratedSink {
  final String name;

  final String uid;

  final Sink<?> sink;

  private DecoratedSink(String name, String uid, Sink<?> sink) {
    this.name = name;
    this.uid = uid;
    this.sink = sink;
  }

  public static DecoratedSink of(EgressSpec<?> spec, Sink<?> sink) {
    EgressIdentifier<?> identifier = spec.id();
    String name = String.format("%s-%s-egress", identifier.namespace(), identifier.name());
    String uid =
        String.format(
            "%s-%s-%s-%s-egress",
            spec.type().namespace(), spec.type().type(), identifier.namespace(), identifier.name());

    return new DecoratedSink(name, uid, sink);
  }
}
