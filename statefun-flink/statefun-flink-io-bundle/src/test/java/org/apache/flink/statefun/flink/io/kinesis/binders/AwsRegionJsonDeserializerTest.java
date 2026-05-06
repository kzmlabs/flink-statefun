// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.io.kinesis.binders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;
import org.junit.jupiter.api.Test;

class AwsRegionJsonDeserializerTest {

  private static final ObjectMapper MAPPER = mapperWithDeserializer();

  @Test
  void defaultTypeProducesDefaultProviderChainRegion() throws Exception {
    AwsRegion region = parse(Map.of("type", "default"));

    assertThat(region.isDefault()).isTrue();
  }

  @Test
  void specificTypeProducesSpecifiedIdRegion() throws Exception {
    AwsRegion region =
        parse(
            Map.of(
                "type", "specific",
                "id", "us-east-1"));

    assertThat(region.isId()).isTrue();
    assertThat(region.asId().id()).isEqualTo("us-east-1");
  }

  @Test
  void customEndpointTypeWithHttpProducesCustomRegion() throws Exception {
    // Pin the LocalStack-friendly behaviour: http:// (not just https://) is accepted.
    AwsRegion region =
        parse(
            Map.of(
                "type", "custom-endpoint",
                "endpoint", "http://localstack:4566",
                "id", "us-east-1"));

    assertThat(region.isCustomEndpoint()).isTrue();
    assertThat(region.asCustomEndpoint().serviceEndpoint()).isEqualTo("http://localstack:4566");
    assertThat(region.asCustomEndpoint().regionId()).isEqualTo("us-east-1");
  }

  @Test
  void customEndpointTypeWithHttpsProducesCustomRegion() throws Exception {
    AwsRegion region =
        parse(
            Map.of(
                "type", "custom-endpoint",
                "endpoint", "https://kinesis.example.com",
                "id", "eu-west-3"));

    assertThat(region.isCustomEndpoint()).isTrue();
    assertThat(region.asCustomEndpoint().serviceEndpoint())
        .isEqualTo("https://kinesis.example.com");
  }

  @Test
  void unknownTypeIsRejectedWithListOfValidValues() {
    assertThatThrownBy(() -> parse(Map.of("type", "us-mars-1")))
        .satisfiesAnyOf(
            t ->
                assertThat(t)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid AWS region type")
                    .hasMessageContaining("default")
                    .hasMessageContaining("specific")
                    .hasMessageContaining("custom-endpoint"),
            t ->
                assertThat(t)
                    .isInstanceOf(JsonMappingException.class)
                    .hasMessageContaining("Invalid AWS region type"));
  }

  private static AwsRegion parse(Map<String, String> fields) throws Exception {
    String json = MAPPER.writeValueAsString(fields);
    return MAPPER.readValue(json, AwsRegion.class);
  }

  private static ObjectMapper mapperWithDeserializer() {
    ObjectMapper m = new ObjectMapper();
    SimpleModule mod = new SimpleModule();
    mod.addDeserializer(AwsRegion.class, new AwsRegionJsonDeserializer());
    m.registerModule(mod);
    return m;
  }
}
