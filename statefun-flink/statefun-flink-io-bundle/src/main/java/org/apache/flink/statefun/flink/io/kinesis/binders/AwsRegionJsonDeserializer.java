// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.io.kinesis.binders;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;

public final class AwsRegionJsonDeserializer extends JsonDeserializer<AwsRegion> {
  private static final String DEFAULT_TYPE = "default";
  private static final String SPECIFIED_ID_TYPE = "specific";
  private static final String CUSTOM_ENDPOINT_TYPE = "custom-endpoint";

  @Override
  public AwsRegion deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException {
    final ObjectNode awsRegionNode = jsonParser.readValueAs(ObjectNode.class);
    final String typeString = awsRegionNode.get("type").asText();

    switch (typeString) {
      case DEFAULT_TYPE:
        return AwsRegion.fromDefaultProviderChain();
      case SPECIFIED_ID_TYPE:
        return AwsRegion.ofId(awsRegionNode.get("id").asText());
      case CUSTOM_ENDPOINT_TYPE:
        return AwsRegion.ofCustomEndpoint(
            awsRegionNode.get("endpoint").asText(), awsRegionNode.get("id").asText());
      default:
        final List<String> validValues =
            Arrays.asList(DEFAULT_TYPE, SPECIFIED_ID_TYPE, CUSTOM_ENDPOINT_TYPE);
        throw new IllegalArgumentException(
            "Invalid AWS region type: "
                + typeString
                + "; valid values are ["
                + String.join(", ", validValues)
                + "]");
    }
  }
}
