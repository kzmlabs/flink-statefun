// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.kinesis.ingress;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.core.OptionalProperty;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kinesis.KinesisIOTypes;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;

public final class KinesisIngressSpec<T> implements IngressSpec<T> {
  private final IngressIdentifier<T> ingressIdentifier;
  private final List<String> streams;

  /**
   * ARN of the single Kinesis stream to consume from (Flink 2.x KinesisStreamsSource API). May be
   * {@code null} when the legacy {@code streams} list is used instead.
   */
  @Nullable private final String streamArn;

  private final KinesisIngressDeserializer<T> deserializer;
  private final KinesisIngressStartupPosition startupPosition;
  private final OptionalProperty<AwsRegion> awsRegion;
  private final OptionalProperty<AwsCredentials> awsCredentials;
  private final Properties properties;

  KinesisIngressSpec(
      IngressIdentifier<T> ingressIdentifier,
      List<String> streams,
      String streamArn,
      KinesisIngressDeserializer<T> deserializer,
      KinesisIngressStartupPosition startupPosition,
      OptionalProperty<AwsRegion> awsRegion,
      OptionalProperty<AwsCredentials> awsCredentials,
      Properties properties) {
    this.ingressIdentifier = Objects.requireNonNull(ingressIdentifier, "ingress identifier");
    this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
    this.startupPosition = Objects.requireNonNull(startupPosition, "startup position");
    this.awsRegion = Objects.requireNonNull(awsRegion, "AWS region configuration");
    this.awsCredentials = Objects.requireNonNull(awsCredentials, "AWS credentials configuration");
    this.properties = Objects.requireNonNull(properties);
    this.streamArn = streamArn;

    this.streams = Objects.requireNonNull(streams, "AWS Kinesis stream names");
    // Invariant enforced by KinesisIngressBuilder.build()
    assert !streams.isEmpty() || streamArn != null;
  }

  @Override
  public IngressIdentifier<T> id() {
    return ingressIdentifier;
  }

  @Override
  public IngressType type() {
    return KinesisIOTypes.UNIVERSAL_INGRESS_TYPE;
  }

  public List<String> streams() {
    return streams;
  }

  /**
   * Returns the ARN of the Kinesis stream to consume from, or {@code null} if the legacy stream
   * name list was used instead.
   */
  @Nullable
  public String streamArn() {
    return streamArn;
  }

  public KinesisIngressDeserializer<T> deserializer() {
    return deserializer;
  }

  public KinesisIngressStartupPosition startupPosition() {
    return startupPosition;
  }

  public OptionalProperty<AwsRegion> awsRegion() {
    return awsRegion;
  }

  public OptionalProperty<AwsCredentials> awsCredentials() {
    return awsCredentials;
  }

  public Properties properties() {
    return properties;
  }
}
