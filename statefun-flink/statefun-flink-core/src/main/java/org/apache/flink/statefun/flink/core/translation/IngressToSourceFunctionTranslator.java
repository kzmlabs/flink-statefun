// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.translation;

import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.common.Maps;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;

final class IngressToSourceFunctionTranslator {
  private final StatefulFunctionsUniverse universe;

  IngressToSourceFunctionTranslator(StatefulFunctionsUniverse universe) {
    this.universe = Objects.requireNonNull(universe);
  }

  Map<IngressIdentifier<?>, DecoratedSource> translate() {
    return Maps.transformValues(universe.ingress(), this::sourceFromSpec);
  }

  private DecoratedSource sourceFromSpec(IngressIdentifier<?> key, IngressSpec<?> spec) {
    SourceProvider provider = universe.sources().get(spec.type());
    if (provider == null) {
      throw new IllegalStateException(
          "Unable to find a source translation for ingress of type "
              + spec.type()
              + ", which is bound for key "
              + key);
    }
    Source<?, ?, ?> source = provider.forSpec(spec);
    if (source == null) {
      throw new NullPointerException(
          "A source provider for type " + spec.type() + ", has produced a NULL source.");
    }
    return DecoratedSource.of(spec, source);
  }
}
