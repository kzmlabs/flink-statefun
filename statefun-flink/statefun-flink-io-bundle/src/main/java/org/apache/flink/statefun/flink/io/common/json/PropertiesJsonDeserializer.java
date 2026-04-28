// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.io.common.json;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public final class PropertiesJsonDeserializer extends JsonDeserializer<Properties> {
  @Override
  public Properties deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    final Iterable<JsonNode> propertyNodes = jsonParser.readValueAs(JsonNode.class);
    final Properties properties = new Properties();
    propertyNodes.forEach(
        jsonNode -> {
          Map.Entry<String, JsonNode> offsetNode = jsonNode.fields().next();
          properties.setProperty(offsetNode.getKey(), offsetNode.getValue().asText());
        });
    return properties;
  }
}
