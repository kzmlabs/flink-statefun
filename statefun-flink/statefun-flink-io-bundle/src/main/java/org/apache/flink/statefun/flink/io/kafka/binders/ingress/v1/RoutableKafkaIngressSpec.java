// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import com.google.protobuf.Message;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.io.common.json.IngressIdentifierJsonDeserializer;
import org.apache.flink.statefun.flink.io.common.json.PropertiesJsonDeserializer;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.generated.TargetFunctionType;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressAutoResetPosition;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilderApiExtension;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressStartupPosition;
import org.apache.flink.statefun.sdk.kafka.KafkaTopicPartition;

@JsonDeserialize(builder = RoutableKafkaIngressSpec.Builder.class)
final class RoutableKafkaIngressSpec {

  private final IngressIdentifier<Message> id;
  private final Optional<String> kafkaAddress;
  private final Optional<String> consumerGroupId;
  private final Map<String, TopicRouting> topicRoutings;
  private final boolean forwardHeaders;
  private final InvalidRecordPolicy invalidRecordHandlingDefault;
  private final KafkaIngressAutoResetPosition autoOffsetResetPosition;
  private final KafkaIngressStartupPosition startupPosition;
  private final Properties properties;

  private RoutableKafkaIngressSpec(
      IngressIdentifier<Message> id,
      Optional<String> kafkaAddress,
      Optional<String> consumerGroupId,
      Map<String, TopicRouting> topicRoutings,
      boolean forwardHeaders,
      InvalidRecordPolicy invalidRecordHandlingDefault,
      KafkaIngressAutoResetPosition autoOffsetResetPosition,
      KafkaIngressStartupPosition startupPosition,
      Properties properties) {
    this.id = id;
    this.kafkaAddress = kafkaAddress;
    this.consumerGroupId = consumerGroupId;
    this.topicRoutings = topicRoutings;
    this.forwardHeaders = forwardHeaders;
    this.invalidRecordHandlingDefault = invalidRecordHandlingDefault;
    this.autoOffsetResetPosition = autoOffsetResetPosition;
    this.startupPosition = startupPosition;
    this.properties = properties;
  }

  public IngressIdentifier<Message> id() {
    return id;
  }

  public org.apache.flink.statefun.sdk.kafka.KafkaIngressSpec toUniversalKafkaIngressSpec() {
    final KafkaIngressBuilder<Message> builder = KafkaIngressBuilder.forIdentifier(id);
    kafkaAddress.ifPresent(builder::withKafkaAddress);
    consumerGroupId.ifPresent(builder::withConsumerGroupId);
    topicRoutings.keySet().forEach(builder::withTopic);
    builder.withAutoResetPosition(autoOffsetResetPosition);
    builder.withStartupPosition(startupPosition);
    builder.withProperties(properties);
    KafkaIngressBuilderApiExtension.withDeserializer(
        builder,
        new RoutableKafkaIngressDeserializer(routingConfigsByTopic(), forwardHeaderTopics(), invalidRecordPolicyByTopic()));

    return builder.build();
  }

  private Map<String, RoutingConfig> routingConfigsByTopic() {
    return topicRoutings.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().config()));
  }

  Set<String> forwardHeaderTopics() {
    return topicRoutings.entrySet().stream()
        .filter(entry -> entry.getValue().forwardsHeaders(forwardHeaders))
        .map(Map.Entry::getKey)
        .collect(Collectors.toUnmodifiableSet());
  }

  Map<String, InvalidRecordPolicy> invalidRecordPolicyByTopic() {
    return topicRoutings.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().invalidRecordPolicy(invalidRecordHandlingDefault)));
  }

  /**
   * A topic routing entry: the routing config plus the topic's optional {@code forwardHeaders}
   * override. The effective per-topic value is the override when present, otherwise the
   * ingress-level {@code forwardHeaders} default (which itself defaults to false — header
   * forwarding is strictly opt-in).
   */
  static final class TopicRouting {
    private final RoutingConfig config;
    private final Boolean forwardHeadersOverride;
    private final InvalidRecordPolicy invalidRecordOverride;

    TopicRouting(RoutingConfig config, Boolean forwardHeadersOverride, InvalidRecordPolicy invalidRecordOverride) {
      this.config = Objects.requireNonNull(config);
      this.forwardHeadersOverride = forwardHeadersOverride;
      this.invalidRecordOverride = invalidRecordOverride;
    }

    RoutingConfig config() {
      return config;
    }

    boolean forwardsHeaders(boolean ingressDefault) {
      return forwardHeadersOverride != null ? forwardHeadersOverride : ingressDefault;
    }

    InvalidRecordPolicy invalidRecordPolicy(InvalidRecordPolicy ingressDefault) {
      return invalidRecordOverride != null ? invalidRecordOverride : ingressDefault;
    }
  }

  @JsonPOJOBuilder
  public static class Builder {

    private final IngressIdentifier<Message> id;

    private Optional<String> kafkaAddress = Optional.empty();
    private Optional<String> consumerGroupId = Optional.empty();
    private Map<String, TopicRouting> topicRoutings = new HashMap<>();
    private boolean forwardHeaders = false;
    private InvalidRecordPolicy invalidRecordHandlingDefault = InvalidRecordPolicy.defaults();
    private KafkaIngressAutoResetPosition autoOffsetResetPosition =
        KafkaIngressAutoResetPosition.LATEST;
    private KafkaIngressStartupPosition startupPosition = KafkaIngressStartupPosition.fromLatest();
    private Properties properties = new Properties();

    @JsonCreator
    private Builder(
        @JsonProperty("id") @JsonDeserialize(using = IngressIdentifierJsonDeserializer.class)
            IngressIdentifier<Message> id) {
      this.id = Objects.requireNonNull(id);
    }

    @JsonProperty("address")
    public Builder withKafkaAddress(String address) {
      Objects.requireNonNull(address);
      this.kafkaAddress = Optional.of(address);
      return this;
    }

    @JsonProperty("consumerGroupId")
    public Builder withConsumerGroupId(String consumerGroupId) {
      Objects.requireNonNull(consumerGroupId);
      this.consumerGroupId = Optional.of(consumerGroupId);
      return this;
    }

    @JsonProperty("topics")
    @JsonDeserialize(using = TopicRoutingsJsonDeserializer.class)
    public Builder withTopicRoutings(Map<String, TopicRouting> topicRoutings) {
      this.topicRoutings = Objects.requireNonNull(topicRoutings);
      return this;
    }

    @JsonProperty("forwardHeaders")
    public Builder withForwardHeaders(boolean forwardHeaders) {
      this.forwardHeaders = forwardHeaders;
      return this;
    }

    @JsonProperty("invalidRecordHandling")
    @JsonDeserialize(using = InvalidRecordHandlingJsonDeserializer.class)
    public Builder withInvalidRecordHandling(InvalidRecordPolicy invalidRecordHandling) {
      this.invalidRecordHandlingDefault = Objects.requireNonNull(invalidRecordHandling);
      return this;
    }

    @JsonProperty("autoOffsetResetPosition")
    @JsonDeserialize(using = AutoOffsetResetPositionJsonDeserializer.class)
    public Builder withAutoOffsetResetPosition(
        KafkaIngressAutoResetPosition autoOffsetResetPosition) {
      this.autoOffsetResetPosition = Objects.requireNonNull(autoOffsetResetPosition);
      return this;
    }

    @JsonProperty("startupPosition")
    @JsonDeserialize(using = StartupPositionJsonDeserializer.class)
    public Builder withStartupPosition(KafkaIngressStartupPosition startupPosition) {
      this.startupPosition = Objects.requireNonNull(startupPosition);
      return this;
    }

    @JsonProperty("properties")
    @JsonDeserialize(using = PropertiesJsonDeserializer.class)
    public Builder withProperties(Properties properties) {
      this.properties = Objects.requireNonNull(properties);
      return this;
    }

    public RoutableKafkaIngressSpec build() {
      return new RoutableKafkaIngressSpec(
          id,
          kafkaAddress,
          consumerGroupId,
          topicRoutings,
          forwardHeaders,
          invalidRecordHandlingDefault,
          autoOffsetResetPosition,
          startupPosition,
          properties);
    }
  }

  private static class TopicRoutingsJsonDeserializer
      extends JsonDeserializer<Map<String, TopicRouting>> {
    @Override
    public Map<String, TopicRouting> deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      final ObjectNode[] routingJsonNodes = jsonParser.readValueAs(ObjectNode[].class);

      final Map<String, TopicRouting> result = new HashMap<>(routingJsonNodes.length);
      for (ObjectNode routingJsonNode : routingJsonNodes) {
        final RoutingConfig routingConfig =
            RoutingConfig.newBuilder()
                .setTypeUrl(routingJsonNode.get("valueType").textValue())
                .addAllTargetFunctionTypes(parseTargetFunctions(routingJsonNode))
                .build();
        result.put(
            routingJsonNode.get("topic").asText(),
            new TopicRouting(routingConfig, parseForwardHeadersOverride(routingJsonNode), parseInvalidRecordOverride(routingJsonNode)));
      }
      return result;
    }

    private static Boolean parseForwardHeadersOverride(ObjectNode routingJsonNode) {
      final JsonNode overrideNode = routingJsonNode.get("forwardHeaders");
      return overrideNode == null || overrideNode.isNull() ? null : overrideNode.asBoolean();
    }

    private static InvalidRecordPolicy parseInvalidRecordOverride(ObjectNode routingJsonNode) {
      final JsonNode overrideNode = routingJsonNode.get("invalidRecordHandling");
      return overrideNode == null || overrideNode.isNull() ? null : InvalidRecordPolicy.fromSpecNode(overrideNode);
    }
  }

  private static class InvalidRecordHandlingJsonDeserializer extends JsonDeserializer<InvalidRecordPolicy> {
    @Override
    public InvalidRecordPolicy deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      return InvalidRecordPolicy.fromSpecNode(jsonParser.readValueAs(ObjectNode.class));
    }
  }

  private static class AutoOffsetResetPositionJsonDeserializer
      extends JsonDeserializer<KafkaIngressAutoResetPosition> {
    @Override
    public KafkaIngressAutoResetPosition deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      try {
        return KafkaIngressAutoResetPosition.valueOf(
            jsonParser.getText().toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Invalid autoOffsetResetPosition: "
                + jsonParser.getText()
                + "; valid values are "
                + Arrays.toString(KafkaIngressAutoResetPosition.values()),
            e);
      }
    }
  }

  private static class StartupPositionJsonDeserializer
      extends JsonDeserializer<KafkaIngressStartupPosition> {
    private static final String STARTUP_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS Z";
    private static final DateTimeFormatter STARTUP_DATE_FORMATTER =
        DateTimeFormatter.ofPattern(STARTUP_DATE_PATTERN);

    @Override
    public KafkaIngressStartupPosition deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      final ObjectNode startupPositionNode = jsonParser.readValueAs(ObjectNode.class);
      final String startupTypeString = startupPositionNode.get("type").asText();
      switch (startupTypeString) {
        case "group-offsets":
          return KafkaIngressStartupPosition.fromGroupOffsets();
        case "earliest":
          return KafkaIngressStartupPosition.fromEarliest();
        case "latest":
          return KafkaIngressStartupPosition.fromLatest();
        case "specific-offsets":
          return KafkaIngressStartupPosition.fromSpecificOffsets(
              parseSpecificStartupOffsetsMap(startupPositionNode));
        case "date":
          return KafkaIngressStartupPosition.fromDate(parseStartupDate(startupPositionNode));
        default:
          throw new IllegalArgumentException(
              "Invalid startup position type: "
                  + startupTypeString
                  + "; valid values are [group-offsets, earliest, latest, specific-offsets, date]");
      }
    }
  }

  private static List<TargetFunctionType> parseTargetFunctions(JsonNode routingJsonNode) {
    final Iterable<JsonNode> targetFunctionNodes = routingJsonNode.get("targets");
    return StreamSupport.stream(targetFunctionNodes.spliterator(), false)
        .map(RoutableKafkaIngressSpec::parseTargetFunctionType)
        .collect(Collectors.toList());
  }

  private static TargetFunctionType parseTargetFunctionType(JsonNode targetFunctionNode) {
    final TypeName targetType = TypeName.parseFrom(targetFunctionNode.asText());
    return TargetFunctionType.newBuilder()
        .setNamespace(targetType.namespace())
        .setType(targetType.name())
        .build();
  }

  private static Map<KafkaTopicPartition, Long> parseSpecificStartupOffsetsMap(
      ObjectNode startupPositionNode) {
    final Iterable<JsonNode> offsetNodes = startupPositionNode.get("offsets");
    final Map<KafkaTopicPartition, Long> offsets = new HashMap<>();
    offsetNodes.forEach(
        jsonNode -> {
          Map.Entry<String, JsonNode> offsetNode = jsonNode.fields().next();
          offsets.put(
              KafkaTopicPartition.fromString(offsetNode.getKey()), offsetNode.getValue().asLong());
        });
    return offsets;
  }

  private static ZonedDateTime parseStartupDate(ObjectNode startupPositionNode) {

    final String dateString = startupPositionNode.get("date").asText();
    try {
      return ZonedDateTime.parse(
          dateString, StartupPositionJsonDeserializer.STARTUP_DATE_FORMATTER);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          "Unable to parse date string for startup position: "
              + dateString
              + "; the date should conform to the pattern "
              + StartupPositionJsonDeserializer.STARTUP_DATE_PATTERN,
          e);
    }
  }
}
