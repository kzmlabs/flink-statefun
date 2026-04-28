// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.translation;

import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.common.Maps;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;

final class EgressToSinkTranslator {
  private final StatefulFunctionsUniverse universe;

  EgressToSinkTranslator(StatefulFunctionsUniverse universe) {
    this.universe = Objects.requireNonNull(universe);
  }

  Map<EgressIdentifier<?>, DecoratedSink> translate() {
    return Maps.transformValues(universe.egress(), this::sinkFromSpec);
  }

  private DecoratedSink sinkFromSpec(EgressIdentifier<?> key, EgressSpec<?> spec) {
    SinkProvider provider = universe.sinks().get(spec.type());
    if (provider == null) {
      throw new IllegalStateException(
          "Unable to find a sink translation for egress of type "
              + spec.type()
              + ", which is bound for key "
              + key);
    }
    Sink<?> sink = provider.forSpec(spec);
    if (sink == null) {
      throw new NullPointerException(
          "A sink provider for type " + spec.type() + ", has produced a NULL sink.");
    }
    return DecoratedSink.of(spec, sink);
  }
}
