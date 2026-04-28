// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.httpfn;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.sdk.TypeName;

public final class TransportClientSpec implements Serializable {

  private static final JsonPointer FACTORY_KIND = JsonPointer.compile("/type");

  public static TransportClientSpec fromJsonNode(ObjectNode node) {
    TypeName factoryKind =
        Selectors.optionalTextAt(node, FACTORY_KIND)
            .map(TypeName::parseFrom)
            .orElse(TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE);

    return new TransportClientSpec(factoryKind, node);
  }

  private final TypeName factoryKind;
  private final ObjectNode specNode;

  public TransportClientSpec(TypeName factoryKind, ObjectNode properties) {
    this.factoryKind = Objects.requireNonNull(factoryKind);
    this.specNode = Objects.requireNonNull(properties);
  }

  public TypeName factoryKind() {
    return factoryKind;
  }

  public ObjectNode specNode() {
    return specNode;
  }
}
