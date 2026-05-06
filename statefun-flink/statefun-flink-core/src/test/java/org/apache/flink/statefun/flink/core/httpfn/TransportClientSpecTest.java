// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.httpfn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.sdk.TypeName;
import org.junit.jupiter.api.Test;

class TransportClientSpecTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void fromJsonWithoutTypeFallsBackToAsyncClientFactoryDefault() {
    ObjectNode node = MAPPER.createObjectNode();

    TransportClientSpec spec = TransportClientSpec.fromJsonNode(node);

    // No "/type" pointer in the JSON -> default factory kind is the async client factory.
    assertThat(spec.factoryKind()).isEqualTo(TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE);
    assertThat(spec.specNode()).isSameAs(node);
  }

  @Test
  void fromJsonWithTypeFieldUsesParsedTypeName() {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("type", "io.test/custom-client");
    node.put("foo", "bar");

    TransportClientSpec spec = TransportClientSpec.fromJsonNode(node);

    assertThat(spec.factoryKind()).isEqualTo(TypeName.parseFrom("io.test/custom-client"));
    // Properties are passed through as-is, including the discriminator field.
    assertThat(spec.specNode().get("foo").asText()).isEqualTo("bar");
  }

  @Test
  void constructorPassesValuesThrough() {
    TypeName factoryKind = TypeName.parseFrom("io.test/custom");
    ObjectNode props = MAPPER.createObjectNode();
    props.put("k", "v");

    TransportClientSpec spec = new TransportClientSpec(factoryKind, props);

    assertThat(spec.factoryKind()).isSameAs(factoryKind);
    assertThat(spec.specNode()).isSameAs(props);
  }

  @Test
  void constructorRejectsNullFactoryKind() {
    assertThatThrownBy(() -> new TransportClientSpec(null, MAPPER.createObjectNode()))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructorRejectsNullProperties() {
    assertThatThrownBy(
            () -> new TransportClientSpec(TypeName.parseFrom("io.test/x"), null))
        .isInstanceOf(NullPointerException.class);
  }
}
