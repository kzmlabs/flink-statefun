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

import java.io.IOException;
import java.util.Objects;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kinesis.source.serialization.KinesisDeserializationSchema;
import org.apache.flink.statefun.flink.common.UnimplementedTypeInfo;
import org.apache.flink.statefun.sdk.kinesis.ingress.IngressRecord;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressDeserializer;
import org.apache.flink.util.Collector;
import software.amazon.awssdk.services.kinesis.model.Record;

/**
 * Bridges a {@link KinesisIngressDeserializer} to the Flink 2.x connector interface {@link
 * KinesisDeserializationSchema}.
 *
 * <p>This implementation uses the Kinesis-specific deserialization interface (rather than the
 * simpler {@code DeserializationSchema<T>}) so that full {@link IngressRecord} metadata — stream
 * name, shard ID, partition key, sequence number, and approximate arrival timestamp — can be
 * surfaced to the user's deserializer. The Flink connector passes stream and shardId as explicit
 * parameters alongside the raw AWS SDK {@link Record}, which carries the remaining metadata fields.
 */
final class KinesisDeserializationSchemaDelegate<T> implements KinesisDeserializationSchema<T> {

  private static final long serialVersionUID = 1L;

  private final TypeInformation<T> producedTypeInfo;
  private final KinesisIngressDeserializer<T> delegate;

  KinesisDeserializationSchemaDelegate(KinesisIngressDeserializer<T> delegate) {
    this.producedTypeInfo = new UnimplementedTypeInfo<>();
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public TypeInformation<T> getProducedType() {
    // This is never actually used; it is replaced during translation with the type information from
    // the IngressIdentifier's producedType. See: Sources#setOutputType. If that invariant breaks,
    // UnimplementedTypeInfo will fail fast.
    return producedTypeInfo;
  }

  @Override
  public void deserialize(Record record, String stream, String shardId, Collector<T> collector)
      throws IOException {
    IngressRecord ingressRecord =
        IngressRecord.newBuilder()
            .withData(record.data().asByteArray())
            .withStream(stream)
            .withShardId(shardId)
            .withPartitionKey(record.partitionKey())
            .withSequenceNumber(record.sequenceNumber())
            .withApproximateArrivalTimestamp(record.approximateArrivalTimestamp().toEpochMilli())
            .build();
    collector.collect(delegate.deserialize(ingressRecord));
  }
}
