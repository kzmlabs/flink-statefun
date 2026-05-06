// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.io.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.statefun.flink.common.UnimplementedTypeInfo;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class KafkaDeserializationSchemaDelegateTest {

  private static final class Utf8Deserializer implements KafkaIngressDeserializer<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> record) {
      return new String(record.value(), StandardCharsets.UTF_8);
    }
  }

  private static final class ListCollector<T> implements Collector<T> {
    final List<T> collected = new ArrayList<>();

    @Override
    public void collect(T record) {
      collected.add(record);
    }

    @Override
    public void close() {}
  }

  @Test
  void delegatesBytesToUserDeserializer() throws IOException {
    KafkaDeserializationSchemaDelegate<String> delegate =
        new KafkaDeserializationSchemaDelegate<>(new Utf8Deserializer());

    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<>(
            "t", 0, 0L, null, "payload".getBytes(StandardCharsets.UTF_8));

    ListCollector<String> collector = new ListCollector<>();
    delegate.deserialize(record, collector);

    assertThat(collector.collected).containsExactly("payload");
  }

  @Test
  void producedTypeReturnsUnimplementedPlaceholder() {
    KafkaDeserializationSchemaDelegate<String> delegate =
        new KafkaDeserializationSchemaDelegate<>(new Utf8Deserializer());

    assertThat(delegate.getProducedType()).isInstanceOf(UnimplementedTypeInfo.class);
  }

  @Test
  void rejectsNullDeserializer() {
    assertThatThrownBy(() -> new KafkaDeserializationSchemaDelegate<>(null))
        .isInstanceOf(NullPointerException.class);
  }
}
