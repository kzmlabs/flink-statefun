// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
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
    // The connector passes the source's stream-ARN string as the {@code stream} argument — NOT the
    // short stream name. RoutableKinesisIngressBinderV1 relies on this: in the ARN path it re-keys
    // its routing map by ARN so that IngressRecord#getStream() lookups at runtime match. Keep this
    // assignment as-is — changing it would break RoutableKinesisIngressDeserializer.
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
