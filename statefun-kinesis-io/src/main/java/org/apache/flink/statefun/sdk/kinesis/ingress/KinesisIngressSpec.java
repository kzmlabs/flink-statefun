/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.sdk.kinesis.ingress;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
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
  private final String streamArn;

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
    if (streams.isEmpty() && streamArn == null) {
      throw new IllegalArgumentException(
          "Must have at least one stream to consume from specified.");
    }
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
