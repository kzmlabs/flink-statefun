// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.kinesis.egress;

import java.util.Objects;
import java.util.Properties;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kinesis.KinesisIOTypes;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;

public final class KinesisEgressSpec<T> implements EgressSpec<T> {
  private final EgressIdentifier<T> egressIdentifier;
  private final Class<? extends KinesisEgressSerializer<T>> serializerClass;
  private final int maxOutstandingRecords;
  private final AwsRegion awsRegion;
  private final AwsCredentials awsCredentials;
  private final Properties clientConfigurationProperties;

  /**
   * Name of the Kinesis stream that the Flink 2.x {@code KinesisStreamsSink} is pre-bound to. Note:
   * {@link EgressRecord#getStream()} is ignored by the runtime when this field is set; all records
   * are written to this single stream.
   */
  private final String streamName;

  KinesisEgressSpec(
      EgressIdentifier<T> egressIdentifier,
      Class<? extends KinesisEgressSerializer<T>> serializerClass,
      int maxOutstandingRecords,
      AwsRegion awsRegion,
      AwsCredentials awsCredentials,
      Properties clientConfigurationProperties,
      String streamName) {
    this.egressIdentifier = Objects.requireNonNull(egressIdentifier);
    this.serializerClass = Objects.requireNonNull(serializerClass);
    this.maxOutstandingRecords = maxOutstandingRecords;
    this.awsRegion = Objects.requireNonNull(awsRegion);
    this.awsCredentials = Objects.requireNonNull(awsCredentials);
    this.clientConfigurationProperties = Objects.requireNonNull(clientConfigurationProperties);
    this.streamName = Objects.requireNonNull(streamName, "stream name");
  }

  @Override
  public EgressIdentifier<T> id() {
    return egressIdentifier;
  }

  @Override
  public EgressType type() {
    return KinesisIOTypes.UNIVERSAL_EGRESS_TYPE;
  }

  public Class<? extends KinesisEgressSerializer<T>> serializerClass() {
    return serializerClass;
  }

  public int maxOutstandingRecords() {
    return maxOutstandingRecords;
  }

  public AwsRegion awsRegion() {
    return awsRegion;
  }

  public AwsCredentials awsCredentials() {
    return awsCredentials;
  }

  public Properties clientConfigurationProperties() {
    return clientConfigurationProperties;
  }

  /**
   * Returns the name of the Kinesis stream that the egress is bound to. This corresponds to the
   * Flink 2.x {@code KinesisStreamsSink} stream name parameter.
   */
  public String streamName() {
    return streamName;
  }
}
