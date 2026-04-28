// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.io.kinesis.binders.ingress.v1;

import com.google.protobuf.Message;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
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
import org.apache.flink.statefun.flink.io.kinesis.binders.AwsCredentialsJsonDeserializer;
import org.apache.flink.statefun.flink.io.kinesis.binders.AwsRegionJsonDeserializer;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressBuilder;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressBuilderApiExtension;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressSpec;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressStartupPosition;

@JsonDeserialize(builder = RoutableKinesisIngressSpec.Builder.class)
public class RoutableKinesisIngressSpec {

  private final IngressIdentifier<Message> id;
  private final AwsRegion awsRegion;
  private final AwsCredentials awsCredentials;
  private final KinesisIngressStartupPosition startupPosition;
  private final String streamArn;
  private final Map<String, RoutingConfig> streamRoutings;
  private final Properties properties;

  private RoutableKinesisIngressSpec(
      IngressIdentifier<Message> id,
      AwsRegion awsRegion,
      AwsCredentials awsCredentials,
      KinesisIngressStartupPosition startupPosition,
      String streamArn,
      Map<String, RoutingConfig> streamRoutings,
      Properties properties) {
    this.id = Objects.requireNonNull(id);
    this.awsRegion = Objects.requireNonNull(awsRegion);
    this.awsCredentials = Objects.requireNonNull(awsCredentials);
    this.startupPosition = Objects.requireNonNull(startupPosition);
    this.streamArn = streamArn;
    this.streamRoutings = Objects.requireNonNull(streamRoutings);
    this.properties = Objects.requireNonNull(properties);
  }

  public KinesisIngressSpec<Message> toUniversalKinesisIngressSpec() {
    final KinesisIngressBuilder<Message> builder =
        KinesisIngressBuilder.forIdentifier(id)
            .withAwsRegion(awsRegion)
            .withAwsCredentials(awsCredentials)
            .withStartupPosition(startupPosition)
            .withProperties(properties);

    if (streamArn != null) {
      // Flink 2.x ARN-based path: single stream identified by ARN; routing config keyed by ARN.
      builder.withStreamArn(streamArn);
      KinesisIngressBuilderApiExtension.withDeserializer(
          builder,
          new RoutableKinesisIngressDeserializer(
              singleArnRoutingConfig(streamArn, streamRoutings)));
    } else {
      // Legacy path: one or more named streams.
      streamRoutings.keySet().forEach(builder::withStream);
      KinesisIngressBuilderApiExtension.withDeserializer(
          builder, new RoutableKinesisIngressDeserializer(streamRoutings));
    }

    return builder.build();
  }

  public IngressIdentifier<Message> id() {
    return id;
  }

  /**
   * When using the ARN path there is only one stream; the routing config is keyed by the ARN string
   * so that the deserializer can look it up by {@code IngressRecord#getStream()}.
   */
  private static Map<String, RoutingConfig> singleArnRoutingConfig(
      String arn, Map<String, RoutingConfig> streamRoutings) {
    if (streamRoutings.size() != 1) {
      throw new IllegalStateException(
          "Exactly one stream routing entry is required when streamArn is set, but found "
              + streamRoutings.size());
    }
    final RoutingConfig config = streamRoutings.values().iterator().next();
    final Map<String, RoutingConfig> result = new HashMap<>(1);
    result.put(arn, config);
    return result;
  }

  @JsonPOJOBuilder
  public static class Builder {
    private final IngressIdentifier<Message> id;

    private AwsRegion awsRegion = AwsRegion.fromDefaultProviderChain();
    private AwsCredentials awsCredentials = AwsCredentials.fromDefaultProviderChain();
    private KinesisIngressStartupPosition startupPosition =
        KinesisIngressStartupPosition.fromLatest();
    private String streamArn;
    private Map<String, RoutingConfig> streamRoutings = new HashMap<>();
    private Properties properties = new Properties();

    @JsonCreator
    private Builder(
        @JsonProperty("id") @JsonDeserialize(using = IngressIdentifierJsonDeserializer.class)
            IngressIdentifier<Message> id) {
      this.id = Objects.requireNonNull(id);
    }

    @JsonProperty("awsRegion")
    @JsonDeserialize(using = AwsRegionJsonDeserializer.class)
    public Builder withAwsRegion(AwsRegion awsRegion) {
      this.awsRegion = Objects.requireNonNull(awsRegion);
      return this;
    }

    @JsonProperty("awsCredentials")
    @JsonDeserialize(using = AwsCredentialsJsonDeserializer.class)
    public Builder withAwsCredentials(AwsCredentials awsCredentials) {
      this.awsCredentials = Objects.requireNonNull(awsCredentials);
      return this;
    }

    @JsonProperty("startupPosition")
    @JsonDeserialize(using = StartupPositionJsonDeserializer.class)
    public Builder withStartupPosition(KinesisIngressStartupPosition startupPosition) {
      this.startupPosition = Objects.requireNonNull(startupPosition);
      return this;
    }

    /** Accepts a full Kinesis stream ARN (Flink 2.x {@code KinesisStreamsSource} path). */
    @JsonProperty("streamArn")
    public Builder withStreamArn(String streamArn) {
      this.streamArn = streamArn;
      return this;
    }

    @JsonProperty("streams")
    @JsonDeserialize(using = StreamRoutingsJsonDeserializer.class)
    public Builder withStreamRoutings(Map<String, RoutingConfig> streamRoutings) {
      this.streamRoutings = Objects.requireNonNull(streamRoutings);
      return this;
    }

    @JsonProperty("clientConfigProperties")
    @JsonDeserialize(using = PropertiesJsonDeserializer.class)
    public Builder withProperties(Properties properties) {
      this.properties = Objects.requireNonNull(properties);
      return this;
    }

    public RoutableKinesisIngressSpec build() {
      return new RoutableKinesisIngressSpec(
          id, awsRegion, awsCredentials, startupPosition, streamArn, streamRoutings, properties);
    }
  }

  private static class StreamRoutingsJsonDeserializer
      extends JsonDeserializer<Map<String, RoutingConfig>> {
    @Override
    public Map<String, RoutingConfig> deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      final ObjectNode[] routingJsonNodes = jsonParser.readValueAs(ObjectNode[].class);

      final Map<String, RoutingConfig> result = new HashMap<>(routingJsonNodes.length);
      for (ObjectNode routingJsonNode : routingJsonNodes) {
        final RoutingConfig routingConfig =
            RoutingConfig.newBuilder()
                .setTypeUrl(routingJsonNode.get("valueType").textValue())
                .addAllTargetFunctionTypes(parseTargetFunctions(routingJsonNode))
                .build();
        result.put(routingJsonNode.get("stream").asText(), routingConfig);
      }
      return result;
    }
  }

  private static class StartupPositionJsonDeserializer
      extends JsonDeserializer<KinesisIngressStartupPosition> {
    private static final String EARLIEST_TYPE = "earliest";
    private static final String LATEST_TYPE = "latest";
    private static final String DATE_TYPE = "date";

    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS Z";
    private static final DateTimeFormatter DATE_FORMATTER =
        DateTimeFormatter.ofPattern(DATE_PATTERN);

    @Override
    public KinesisIngressStartupPosition deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      final ObjectNode startupPositionNode = jsonParser.readValueAs(ObjectNode.class);
      final String startupTypeString = startupPositionNode.get("type").asText();
      switch (startupTypeString) {
        case EARLIEST_TYPE:
          return KinesisIngressStartupPosition.fromEarliest();
        case LATEST_TYPE:
          return KinesisIngressStartupPosition.fromLatest();
        case DATE_TYPE:
          return KinesisIngressStartupPosition.fromDate(parseStartupDate(startupPositionNode));
        default:
          final List<String> validValues = Arrays.asList(EARLIEST_TYPE, LATEST_TYPE, DATE_TYPE);
          throw new IllegalArgumentException(
              "Invalid startup position type: "
                  + startupTypeString
                  + "; valid values are ["
                  + String.join(", ", validValues)
                  + "]");
      }
    }
  }

  private static List<TargetFunctionType> parseTargetFunctions(JsonNode routingJsonNode) {
    final Iterable<JsonNode> targetFunctionNodes = routingJsonNode.get("targets");
    return StreamSupport.stream(targetFunctionNodes.spliterator(), false)
        .map(RoutableKinesisIngressSpec::parseTargetFunctionType)
        .collect(Collectors.toList());
  }

  private static TargetFunctionType parseTargetFunctionType(JsonNode targetFunctionNode) {
    final TypeName targetType = TypeName.parseFrom(targetFunctionNode.asText());
    return TargetFunctionType.newBuilder()
        .setNamespace(targetType.namespace())
        .setType(targetType.name())
        .build();
  }

  private static ZonedDateTime parseStartupDate(ObjectNode startupPositionNode) {

    final String dateString = startupPositionNode.get("date").asText();
    try {
      return ZonedDateTime.parse(dateString, StartupPositionJsonDeserializer.DATE_FORMATTER);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          "Unable to parse date string for startup position: "
              + dateString
              + "; the date should conform to the pattern "
              + StartupPositionJsonDeserializer.DATE_PATTERN,
          e);
    }
  }
}
