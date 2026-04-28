// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.translation;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;

final class DecoratedSource {
  final String name;

  final String uid;

  final Source<?, ?, ?> source;

  private DecoratedSource(String name, String uid, Source<?, ?, ?> source) {
    this.name = name;
    this.uid = uid;
    this.source = source;
  }

  public static DecoratedSource of(IngressSpec<?> spec, Source<?, ?, ?> source) {
    IngressIdentifier<?> identifier = spec.id();
    String name = String.format("%s-%s-ingress", identifier.namespace(), identifier.name());
    String uid =
        String.format(
            "%s-%s-%s-%s-ingress",
            spec.type().namespace(), spec.type().type(), identifier.namespace(), identifier.name());

    return new DecoratedSource(name, uid, source);
  }
}
