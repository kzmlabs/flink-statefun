// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.io.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class KafkaSerializationSchemaDelegateTest {

  private static final class IdentitySerializer implements KafkaEgressSerializer<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String value) {
      return new ProducerRecord<>(
          "topic", null, "k".getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  void delegatesToUserSerializer() {
    KafkaSerializationSchemaDelegate<String> delegate =
        new KafkaSerializationSchemaDelegate<>(new IdentitySerializer());

    ProducerRecord<byte[], byte[]> record = delegate.serialize("hello", null, 0L);

    assertThat(record.topic()).isEqualTo("topic");
    assertThat(new String(record.value(), StandardCharsets.UTF_8)).isEqualTo("hello");
    assertThat(new String(record.key(), StandardCharsets.UTF_8)).isEqualTo("k");
  }

  @Test
  void rejectsNullSerializer() {
    assertThatThrownBy(() -> new KafkaSerializationSchemaDelegate<>(null))
        .isInstanceOf(NullPointerException.class);
  }
}
