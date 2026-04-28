// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.io.kinesis.binders;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;

public final class AwsCredentialsJsonDeserializer extends JsonDeserializer<AwsCredentials> {
  private static final String DEFAULT_TYPE = "default";
  private static final String BASIC_TYPE = "basic";
  private static final String PROFILE_TYPE = "profile";

  @Override
  public AwsCredentials deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    final ObjectNode awsCredentialsNode = jsonParser.readValueAs(ObjectNode.class);
    final String typeString = awsCredentialsNode.get("type").asText();

    switch (typeString) {
      case DEFAULT_TYPE:
        return AwsCredentials.fromDefaultProviderChain();
      case BASIC_TYPE:
        return AwsCredentials.basic(
            awsCredentialsNode.get("accessKeyId").asText(),
            awsCredentialsNode.get("secretAccessKey").asText());
      case PROFILE_TYPE:
        final JsonNode pathNode = awsCredentialsNode.get("profilePath");
        if (pathNode != null) {
          return AwsCredentials.profile(
              awsCredentialsNode.get("profileName").asText(), pathNode.asText());
        } else {
          return AwsCredentials.profile(awsCredentialsNode.get("profileName").asText());
        }
      default:
        final List<String> validValues = Arrays.asList(DEFAULT_TYPE, BASIC_TYPE, PROFILE_TYPE);
        throw new IllegalArgumentException(
            "Invalid AWS credential type: "
                + typeString
                + "; valid values are ["
                + String.join(", ", validValues)
                + "]");
    }
  }
}
