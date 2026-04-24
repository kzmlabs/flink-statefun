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
package org.apache.flink.statefun.flink.io.kinesis;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;
import org.apache.flink.statefun.sdk.kinesis.egress.EgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;

/**
 * Bridges a {@link KinesisEgressSerializer} to the two-part interface required by Flink 2.x {@code
 * KinesisStreamsSink}: a {@link SerializationSchema} for the record bytes and a {@link
 * PartitionKeyGenerator} for the routing key.
 *
 * <p>Because {@code KinesisStreamsSink} accepts {@code setSerializationSchema} and {@code
 * setPartitionKeyGenerator} as separate concerns, we invoke the user serializer twice per element —
 * once to produce the bytes and once to produce the partition key. This is acceptable given that
 * user serializers are expected to be lightweight (protobuf / JSON encoding). If double-invocation
 * were a concern, a {@code ThreadLocal} cache keyed by reference identity could be introduced.
 *
 * <p>The {@link EgressRecord#getStream()} field returned by the user serializer is intentionally
 * ignored here: the Flink {@code KinesisStreamsSink} is pre-bound to a single stream via {@code
 * setStreamName()} / {@code setStreamArn()} on its builder, so per-record stream routing is not
 * supported by the standard sink API. Similarly, {@link EgressRecord#getExplicitHashKey()} is not
 * forwarded; the standard sink builder does not expose a per-record explicit hash key.
 */
final class KinesisSerializationSchemaDelegate<T> implements SerializationSchema<T>, Serializable {

  private static final long serialVersionUID = 1L;

  private final KinesisEgressSerializer<T> serializer;

  KinesisSerializationSchemaDelegate(KinesisEgressSerializer<T> serializer) {
    this.serializer = Objects.requireNonNull(serializer);
  }

  @Override
  public byte[] serialize(T element) {
    return serializer.serialize(element).getData();
  }

  /**
   * Returns a {@link PartitionKeyGenerator} backed by the same user serializer.
   *
   * <p>The returned generator is serializable (required by Flink's operator state mechanism) and
   * calls the user serializer independently of {@link #serialize(Object)}.
   */
  PartitionKeyGenerator<T> partitionKeyGenerator() {
    return new SerializerBackedPartitionKeyGenerator<>(serializer);
  }

  private static final class SerializerBackedPartitionKeyGenerator<T>
      implements PartitionKeyGenerator<T>, Serializable {

    private static final long serialVersionUID = 1L;

    private final KinesisEgressSerializer<T> serializer;

    SerializerBackedPartitionKeyGenerator(KinesisEgressSerializer<T> serializer) {
      this.serializer = serializer;
    }

    @Override
    public String apply(T element) {
      EgressRecord egressRecord = serializer.serialize(element);
      return egressRecord.getPartitionKey();
    }
  }
}
