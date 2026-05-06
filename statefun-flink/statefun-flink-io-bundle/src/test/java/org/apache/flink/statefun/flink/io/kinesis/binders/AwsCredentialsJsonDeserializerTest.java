// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.io.kinesis.binders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.junit.jupiter.api.Test;

class AwsCredentialsJsonDeserializerTest {

  private static final ObjectMapper MAPPER = mapperWithDeserializer();

  @Test
  void defaultTypeProducesDefaultProviderChainCredentials() throws Exception {
    AwsCredentials creds = parse(Map.of("type", "default"));

    assertThat(creds.isDefault()).isTrue();
  }

  @Test
  void basicTypeProducesBasicCredentials() throws Exception {
    AwsCredentials creds =
        parse(
            Map.of(
                "type", "basic",
                "accessKeyId", "AKIAFAKE",
                "secretAccessKey", "secret/FAKE"));

    assertThat(creds.isBasic()).isTrue();
    assertThat(creds.asBasic().accessKeyId()).isEqualTo("AKIAFAKE");
    assertThat(creds.asBasic().secretAccessKey()).isEqualTo("secret/FAKE");
  }

  @Test
  void profileTypeWithoutPathProducesProfileCredentials() throws Exception {
    AwsCredentials creds =
        parse(
            Map.of(
                "type", "profile",
                "profileName", "default"));

    assertThat(creds.isProfile()).isTrue();
    assertThat(creds.asProfile().name()).isEqualTo("default");
  }

  @Test
  void profileTypeWithPathProducesProfileCredentialsWithPath() throws Exception {
    AwsCredentials creds =
        parse(
            Map.of(
                "type", "profile",
                "profileName", "qa",
                "profilePath", "/etc/aws/credentials"));

    assertThat(creds.isProfile()).isTrue();
    assertThat(creds.asProfile().name()).isEqualTo("qa");
    assertThat(creds.asProfile().path()).isPresent().contains("/etc/aws/credentials");
  }

  @Test
  void unknownTypeIsRejectedWithListOfValidValues() {
    assertThatThrownBy(() -> parse(Map.of("type", "iam-role")))
        .satisfiesAnyOf(
            t ->
                assertThat(t)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid AWS credential type")
                    .hasMessageContaining("default")
                    .hasMessageContaining("basic")
                    .hasMessageContaining("profile"),
            t ->
                assertThat(t)
                    .isInstanceOf(JsonMappingException.class)
                    .hasMessageContaining("Invalid AWS credential type"));
  }

  private static AwsCredentials parse(Map<String, String> fields) throws Exception {
    String json = MAPPER.writeValueAsString(fields);
    return MAPPER.readValue(json, AwsCredentials.class);
  }

  private static ObjectMapper mapperWithDeserializer() {
    ObjectMapper m = new ObjectMapper();
    SimpleModule mod = new SimpleModule();
    mod.addDeserializer(AwsCredentials.class, new AwsCredentialsJsonDeserializer());
    m.registerModule(mod);
    return m;
  }
}
