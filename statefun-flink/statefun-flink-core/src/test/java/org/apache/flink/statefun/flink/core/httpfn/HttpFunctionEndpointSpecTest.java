// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.httpfn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.TypeName;
import org.junit.jupiter.api.Test;

/**
 * Pins the JSON contract of {@link HttpFunctionEndpointSpec} — the public schema that
 * `module.yaml` files declare HTTP endpoints with. This is load-bearing: a regression here
 * silently changes how user-authored module specs are interpreted.
 */
class HttpFunctionEndpointSpecTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void minimalEndpointJsonProducesSpecWithDefaults() throws Exception {
    String json =
        "{"
            + "\"functions\": \"counter/inc\","
            + "\"urlPathTemplate\": \"http://upstream/api\""
            + "}";

    HttpFunctionEndpointSpec spec = readSpec(json);

    assertThat(spec.targetFunctions().isSpecificFunctionType()).isTrue();
    assertThat(spec.targetFunctions().asSpecificFunctionType())
        .isEqualTo(new FunctionType("counter", "inc"));
    assertThat(spec.urlPathTemplate().apply(new FunctionType("counter", "inc")))
        .isEqualTo(URI.create("http://upstream/api"));
    // Default batch limit is 1000.
    assertThat(spec.maxNumBatchRequests()).isEqualTo(1000);
    // Default transport client kind is the async client factory.
    assertThat(spec.transportClientFactoryType())
        .isEqualTo(TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE);
  }

  @Test
  void wildcardFunctionsPatternProducesNamespaceMatcher() throws Exception {
    String json =
        "{"
            + "\"functions\": \"counter/*\","
            + "\"urlPathTemplate\": \"http://upstream/api/{function.name}\""
            + "}";

    HttpFunctionEndpointSpec spec = readSpec(json);

    assertThat(spec.targetFunctions().isNamespace()).isTrue();
    assertThat(spec.targetFunctions().asNamespace().targetNamespace()).isEqualTo("counter");
  }

  @Test
  void maxNumBatchRequestsOverridesDefault() throws Exception {
    String json =
        "{"
            + "\"functions\": \"counter/inc\","
            + "\"urlPathTemplate\": \"http://upstream\","
            + "\"maxNumBatchRequests\": 4096"
            + "}";

    HttpFunctionEndpointSpec spec = readSpec(json);

    assertThat(spec.maxNumBatchRequests()).isEqualTo(4096);
  }

  @Test
  void transportBlockSetsClientFactoryKindAndPropagatesProperties() throws Exception {
    // Pin the wire format that v2 binders expect: a top-level `transport` object with `type`
    // pointing to a custom transport-client factory.
    String json =
        "{"
            + "\"functions\": \"counter/inc\","
            + "\"urlPathTemplate\": \"http://upstream\","
            + "\"transport\": {"
            + "  \"type\": \"io.test/custom-client\","
            + "  \"timeouts\": { \"call\": \"5min\" }"
            + "}"
            + "}";

    HttpFunctionEndpointSpec spec = readSpec(json);

    assertThat(spec.transportClientFactoryType())
        .isEqualTo(TypeName.parseFrom("io.test/custom-client"));
    // Properties block forwarded to the transport client; pin a sample field.
    assertThat(spec.transportClientProperties().get("timeouts").get("call").asText())
        .isEqualTo("5min");
  }

  @Test
  void absentTransportFallsBackToDefaultAsyncClientFactory() throws Exception {
    HttpFunctionEndpointSpec spec =
        readSpec(
            "{\"functions\":\"counter/inc\",\"urlPathTemplate\":\"http://upstream\"}");

    assertThat(spec.transportClientFactoryType())
        .isEqualTo(TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE);
  }

  @Test
  void invalidFunctionsPatternFailsAtDeserialization() {
    // Pin: a comma-list pattern in `functions` is rejected by the deserializer so misuse fails
    // loud rather than silently dropping bindings (real-world bug class).
    String json =
        "{\"functions\": \"counter/a, counter/b\",\"urlPathTemplate\":\"http://upstream\"}";

    assertThatThrownBy(() -> readSpec(json)).isNotNull();
  }

  @Test
  void builderRejectsNullTransportSpec() {
    HttpFunctionEndpointSpec.Builder builder =
        HttpFunctionEndpointSpec.builder(
            TargetFunctions.fromPatternString("counter/inc"),
            new UrlPathTemplate("http://upstream"));

    assertThatThrownBy(() -> builder.withTransport((TransportClientSpec) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void builderProducesSpecWithCustomTransportClientSpec() {
    TransportClientSpec custom =
        new TransportClientSpec(
            TypeName.parseFrom("io.test/custom-client"),
            new ObjectMapper().createObjectNode().put("k", "v"));

    HttpFunctionEndpointSpec spec =
        HttpFunctionEndpointSpec.builder(
                TargetFunctions.fromPatternString("counter/inc"),
                new UrlPathTemplate("http://upstream"))
            .withTransport(custom)
            .build();

    assertThat(spec.transportClientFactoryType())
        .isEqualTo(TypeName.parseFrom("io.test/custom-client"));
    assertThat(spec.transportClientProperties().get("k").asText()).isEqualTo("v");
  }

  private HttpFunctionEndpointSpec readSpec(String json) throws Exception {
    return mapper.readValue(json, HttpFunctionEndpointSpec.class);
  }
}
