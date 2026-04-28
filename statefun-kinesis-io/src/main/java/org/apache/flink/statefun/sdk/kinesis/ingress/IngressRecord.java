// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.kinesis.ingress;

import java.util.Objects;

/** A record consumed from AWS Kinesis. */
public final class IngressRecord {
  private final byte[] data;
  private final String stream;
  private final String shardId;
  private final String partitionKey;
  private final String sequenceNumber;
  private final long approximateArrivalTimestamp;

  /** @return A builder for a {@link IngressRecord}. */
  public static Builder newBuilder() {
    return new Builder();
  }

  private IngressRecord(
      byte[] data,
      String stream,
      String shardId,
      String partitionKey,
      String sequenceNumber,
      long approximateArrivalTimestamp) {
    this.data = Objects.requireNonNull(data, "data bytes");
    this.stream = Objects.requireNonNull(stream, "source stream");
    this.shardId = Objects.requireNonNull(shardId, "source shard id");
    this.partitionKey = Objects.requireNonNull(partitionKey, "partition key");
    this.sequenceNumber = Objects.requireNonNull(sequenceNumber, "sequence number");
    this.approximateArrivalTimestamp = approximateArrivalTimestamp;
  }

  /** @return consumed data bytes */
  public byte[] getData() {
    return data;
  }

  /** @return source AWS Kinesis stream */
  public String getStream() {
    return stream;
  }

  /** @return source AWS Kinesis stream shard */
  public String getShardId() {
    return shardId;
  }

  /** @return attached partition key */
  public String getPartitionKey() {
    return partitionKey;
  }

  /** @return sequence number of the consumed record */
  public String getSequenceNumber() {
    return sequenceNumber;
  }

  /**
   * @return approximate arrival timestamp (ingestion time at AWS Kinesis) of the consumed record
   */
  public long getApproximateArrivalTimestamp() {
    return approximateArrivalTimestamp;
  }

  /** Builder for {@link IngressRecord}. */
  public static final class Builder {
    private byte[] data;
    private String stream;
    private String shardId;
    private String partitionKey;
    private String sequenceNumber;
    long approximateArrivalTimestamp;

    private Builder() {}

    public Builder withData(byte[] data) {
      this.data = data;
      return this;
    }

    public Builder withStream(String stream) {
      this.stream = stream;
      return this;
    }

    public Builder withShardId(String shardId) {
      this.shardId = shardId;
      return this;
    }

    public Builder withPartitionKey(String partitionKey) {
      this.partitionKey = partitionKey;
      return this;
    }

    public Builder withSequenceNumber(String sequenceNumber) {
      this.sequenceNumber = sequenceNumber;
      return this;
    }

    public Builder withApproximateArrivalTimestamp(long approximateArrivalTimestamp) {
      this.approximateArrivalTimestamp = approximateArrivalTimestamp;
      return this;
    }

    public IngressRecord build() {
      return new IngressRecord(
          data, stream, shardId, partitionKey, sequenceNumber, approximateArrivalTimestamp);
    }
  }
}
