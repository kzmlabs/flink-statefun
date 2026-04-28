// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.io.kinesis.binders.egress.v1;

import java.util.Objects;
import java.util.Properties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.flink.statefun.flink.io.common.json.EgressIdentifierJsonDeserializer;
import org.apache.flink.statefun.flink.io.common.json.PropertiesJsonDeserializer;
import org.apache.flink.statefun.flink.io.kinesis.binders.AwsCredentialsJsonDeserializer;
import org.apache.flink.statefun.flink.io.kinesis.binders.AwsRegionJsonDeserializer;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressBuilder;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

@JsonDeserialize(builder = GenericKinesisEgressSpec.Builder.class)
public final class GenericKinesisEgressSpec {

  private final EgressIdentifier<TypedValue> id;
  private final String streamName;
  private final AwsRegion awsRegion;
  private final AwsCredentials awsCredentials;
  private final int maxOutstandingRecords;
  private final Properties properties;

  private GenericKinesisEgressSpec(
      EgressIdentifier<TypedValue> id,
      String streamName,
      AwsRegion awsRegion,
      AwsCredentials awsCredentials,
      int maxOutstandingRecords,
      Properties properties) {
    this.id = Objects.requireNonNull(id);
    this.streamName = Objects.requireNonNull(streamName);
    this.awsRegion = Objects.requireNonNull(awsRegion);
    this.awsCredentials = Objects.requireNonNull(awsCredentials);
    this.maxOutstandingRecords = maxOutstandingRecords;
    this.properties = Objects.requireNonNull(properties);
  }

  public KinesisEgressSpec<TypedValue> toUniversalKinesisEgressSpec() {
    final KinesisEgressBuilder<TypedValue> builder =
        KinesisEgressBuilder.forIdentifier(id)
            .withStreamName(streamName)
            .withAwsRegion(awsRegion)
            .withAwsCredentials(awsCredentials)
            .withMaxOutstandingRecords(maxOutstandingRecords)
            .withProperties(properties)
            .withSerializer(GenericKinesisEgressSerializer.class);
    return builder.build();
  }

  public EgressIdentifier<TypedValue> id() {
    return id;
  }

  @JsonPOJOBuilder
  public static class Builder {
    private final EgressIdentifier<TypedValue> id;
    private String streamName;

    private AwsRegion awsRegion = AwsRegion.fromDefaultProviderChain();
    private AwsCredentials awsCredentials = AwsCredentials.fromDefaultProviderChain();
    private int maxOutstandingRecords = 1000;
    private Properties properties = new Properties();

    @JsonCreator
    private Builder(
        @JsonProperty("id") @JsonDeserialize(using = EgressIdentifierJsonDeserializer.class)
            EgressIdentifier<TypedValue> id) {
      this.id = Objects.requireNonNull(id);
    }

    @JsonProperty("streamName")
    public Builder withStreamName(String streamName) {
      this.streamName = Objects.requireNonNull(streamName);
      return this;
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

    @JsonProperty("maxOutstandingRecords")
    public Builder withMaxOutstandingRecords(int maxOutstandingRecords) {
      this.maxOutstandingRecords = maxOutstandingRecords;
      return this;
    }

    @JsonProperty("clientConfigProperties")
    @JsonDeserialize(using = PropertiesJsonDeserializer.class)
    public Builder withProperties(Properties properties) {
      this.properties = Objects.requireNonNull(properties);
      return this;
    }

    public GenericKinesisEgressSpec build() {
      if (streamName == null) {
        throw new IllegalArgumentException(
            "spec.streamName is required for io.statefun.kinesis.v1/egress");
      }
      return new GenericKinesisEgressSpec(
          id, streamName, awsRegion, awsCredentials, maxOutstandingRecords, properties);
    }
  }
}
