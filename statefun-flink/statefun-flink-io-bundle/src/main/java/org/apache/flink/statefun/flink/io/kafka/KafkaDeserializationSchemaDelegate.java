// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.io.kafka;

import java.util.Objects;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.statefun.flink.common.UnimplementedTypeInfo;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

final class KafkaDeserializationSchemaDelegate<T> implements KafkaRecordDeserializationSchema<T> {

  private static final long serialVersionUID = 1;

  private final TypeInformation<T> producedTypeInfo;
  private final KafkaIngressDeserializer<T> delegate;

  KafkaDeserializationSchemaDelegate(KafkaIngressDeserializer<T> delegate) {
    this.producedTypeInfo = new UnimplementedTypeInfo<>();
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public TypeInformation<T> getProducedType() {
    // this would never be actually used, it would be replaced during translation with the type
    // information
    // of IngressIdentifier's producedType.
    // see: Sources#setOutputType.
    // if this invriant would not hold in the future, this type information would produce a
    // serialier
    // that fails immediately.
    return producedTypeInfo;
  }

  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<T> collector) {
    collector.collect(delegate.deserialize(consumerRecord));
  }
}
