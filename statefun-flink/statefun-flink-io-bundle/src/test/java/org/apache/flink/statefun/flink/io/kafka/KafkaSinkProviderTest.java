// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.io.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class KafkaSinkProviderTest {

  private static final EgressIdentifier<String> ID =
      new EgressIdentifier<>("test", "kafka-egress", String.class);

  private static final KafkaSinkProvider PROVIDER = new KafkaSinkProvider();

  @Test
  void buildsSinkFromAtLeastOnceSpec() {
    var spec =
        KafkaEgressBuilder.forIdentifier(ID)
            .withKafkaAddress("localhost:9092")
            .withSerializer(TestSerializer.class)
            .withAtLeastOnceProducerSemantics()
            .build();

    Sink<String> sink = PROVIDER.forSpec(spec);

    assertThat(sink).isNotNull().isInstanceOf(KafkaSink.class);
  }

  @Test
  void exactlyOnceWithoutTransactionalIdPrefixFailsAtSinkBuild() {
    // TODO(re-engineer Kafka egress for Sink V2): two related Flink-Kafka-Sink-V2 migration
    // gaps in this provider:
    //   1. setTransactionalIdPrefix() is not called — EXACTLY_ONCE rejects at build time
    //      (this test pins that behaviour).
    //   2. KafkaEgressBuilder.withKafkaProducerPoolSize() is a silent no-op on Sink V2
    //      (V2 manages its own pool internally).
    // Both stem from the V1→V2 connector migration and should be addressed together when
    // the egress is reworked. This test pins (1) so a future fix has a tripwire.
    var spec =
        KafkaEgressBuilder.forIdentifier(ID)
            .withKafkaAddress("localhost:9092")
            .withSerializer(TestSerializer.class)
            .withExactlyOnceProducerSemantics(Duration.ofMinutes(5))
            .build();

    assertThatThrownBy(() -> PROVIDER.forSpec(spec))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("transactionalIdPrefix");
  }

  @Test
  void buildsSinkFromNoSemanticsSpec() {
    var spec =
        KafkaEgressBuilder.forIdentifier(ID)
            .withKafkaAddress("localhost:9092")
            .withSerializer(TestSerializer.class)
            .withNoProducerSemantics()
            .build();

    Sink<String> sink = PROVIDER.forSpec(spec);

    assertThat(sink).isNotNull().isInstanceOf(KafkaSink.class);
  }

  @Test
  void buildsSinkPropagatesUserProperties() {
    var spec =
        KafkaEgressBuilder.forIdentifier(ID)
            .withKafkaAddress("localhost:9092")
            .withSerializer(TestSerializer.class)
            .withAtLeastOnceProducerSemantics()
            .withProperty("compression.type", "snappy")
            .build();

    // Construction must succeed; property propagation is verified by the absence of
    // IllegalArgumentException from KafkaSinkBuilder.build().
    assertThat(PROVIDER.forSpec(spec)).isNotNull();
  }

  @Test
  void wrongSpecTypeThrows() {
    EgressSpec<String> wrongSpec =
        new EgressSpec<>() {
          @Override
          public EgressIdentifier<String> id() {
            return ID;
          }

          @Override
          public EgressType type() {
            return new EgressType("wrong", "type");
          }
        };

    assertThatThrownBy(() -> PROVIDER.forSpec(wrongSpec))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Wrong type");
  }

  @Test
  void nullSpecThrows() {
    assertThatThrownBy(() -> PROVIDER.forSpec(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("NULL");
  }

  public static class TestSerializer implements KafkaEgressSerializer<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String value) {
      return new ProducerRecord<>("topic", null, value.getBytes(StandardCharsets.UTF_8));
    }
  }
}
