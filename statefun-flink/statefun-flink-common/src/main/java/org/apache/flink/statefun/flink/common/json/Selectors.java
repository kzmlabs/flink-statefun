// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.common.json;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.TimeUtils;

public final class Selectors {

  public static Optional<ObjectNode> optionalObjectAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return Optional.empty();
    }
    if (!node.isObject()) {
      throw new WrongTypeException(pointer, "not an object");
    }
    return Optional.of((ObjectNode) node);
  }

  public static String textAt(JsonNode node, JsonPointer pointer) {
    node = dereference(node, pointer);
    if (!node.isTextual()) {
      throw new WrongTypeException(pointer, "not a string");
    }
    return node.asText();
  }

  public static Optional<String> optionalTextAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return Optional.empty();
    }
    if (!node.isTextual()) {
      throw new WrongTypeException(pointer, "not a string");
    }
    return Optional.of(node.asText());
  }

  public static Duration durationAt(JsonNode node, JsonPointer pointer) {
    node = dereference(node, pointer);
    if (!node.isTextual()) {
      throw new WrongTypeException(pointer, "not a duration");
    }

    try {
      return TimeUtils.parseDuration(node.asText());
    } catch (IllegalArgumentException ignore) {
      throw new WrongTypeException(pointer, "not a duration");
    }
  }

  public static Optional<Duration> optionalDurationAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return Optional.empty();
    }
    if (!node.isTextual()) {
      throw new WrongTypeException(pointer, "not a duration");
    }

    try {
      Duration duration = TimeUtils.parseDuration(node.asText());
      return Optional.of(duration);
    } catch (IllegalArgumentException ignore) {
      throw new WrongTypeException(pointer, "not a duration");
    }
  }

  public static int integerAt(JsonNode node, JsonPointer pointer) {
    node = dereference(node, pointer);
    if (!node.isInt()) {
      throw new WrongTypeException(pointer, "not an integer");
    }
    return node.asInt();
  }

  public static long longAt(JsonNode node, JsonPointer pointer) {
    node = dereference(node, pointer);
    if (!node.isLong() && !node.isInt()) {
      throw new WrongTypeException(pointer, "not a long");
    }
    return node.asLong();
  }

  public static OptionalLong optionalLongAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return OptionalLong.empty();
    }

    if (!node.isLong() && !node.isInt()) {
      throw new WrongTypeException(pointer, "not a long");
    }
    return OptionalLong.of(node.asLong());
  }

  public static OptionalInt optionalIntegerAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return OptionalInt.empty();
    }
    if (!node.isInt()) {
      throw new WrongTypeException(pointer, "not an integer");
    }
    return OptionalInt.of(node.asInt());
  }

  public static Iterable<? extends JsonNode> listAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return Collections.emptyList();
    }
    if (!node.isArray()) {
      throw new WrongTypeException(pointer, "not a list");
    }
    return node;
  }

  public static List<String> textListAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return Collections.emptyList();
    }
    if (!node.isArray()) {
      throw new WrongTypeException(pointer, "not a list");
    }
    return StreamSupport.stream(node.spliterator(), false)
        .filter(JsonNode::isTextual)
        .map(JsonNode::asText)
        .collect(Collectors.toList());
  }

  public static Map<String, String> propertiesAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return Collections.emptyMap();
    }
    if (!node.isArray()) {
      throw new WrongTypeException(pointer, "not a key-value list");
    }
    Map<String, String> properties = new LinkedHashMap<>();
    for (JsonNode listElement : node) {
      Iterator<Map.Entry<String, JsonNode>> fields = listElement.fields();
      if (!fields.hasNext()) {
        throw new WrongTypeException(pointer, "not a key-value list");
      }
      Map.Entry<String, JsonNode> field = fields.next();
      properties.put(field.getKey(), field.getValue().asText());
    }
    return properties;
  }

  public static Map<String, Long> longPropertiesAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return Collections.emptyMap();
    }
    if (!node.isArray()) {
      throw new WrongTypeException(pointer, "not a key-value list");
    }
    Map<String, Long> longProperties = new LinkedHashMap<>();
    for (JsonNode listElement : node) {
      Iterator<Map.Entry<String, JsonNode>> fields = listElement.fields();
      if (!fields.hasNext()) {
        throw new WrongTypeException(pointer, "not a key-value list");
      }
      Map.Entry<String, JsonNode> field = fields.next();
      if (!field.getValue().isLong() && !field.getValue().isInt()) {
        throw new WrongTypeException(
            pointer,
            "value for key-value pair at "
                + field.getKey()
                + " is not a long: "
                + field.getValue());
      }
      longProperties.put(field.getKey(), field.getValue().asLong());
    }
    return longProperties;
  }

  private static JsonNode dereference(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      throw new MissingKeyException(pointer);
    }
    return node;
  }
}
