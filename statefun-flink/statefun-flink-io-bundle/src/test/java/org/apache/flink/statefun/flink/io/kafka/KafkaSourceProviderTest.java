// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.io.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressAutoResetPosition;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressStartupPosition;
import org.apache.flink.statefun.sdk.kafka.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class KafkaSourceProviderTest {

  private static final IngressIdentifier<String> ID =
      new IngressIdentifier<>(String.class, "test", "kafka-ingress");

  private static final KafkaSourceProvider PROVIDER = new KafkaSourceProvider();

  @Test
  void buildsSourceFromGroupOffsetsStartup() {
    var spec =
        KafkaIngressBuilder.forIdentifier(ID)
            .withKafkaAddress("localhost:9092")
            .withConsumerGroupId("g1")
            .withTopic("t1")
            .withDeserializer(NoopDeserializer.class)
            .withStartupPosition(KafkaIngressStartupPosition.fromGroupOffsets())
            .withAutoResetPosition(KafkaIngressAutoResetPosition.EARLIEST)
            .build();

    Source<String, ?, ?> source = PROVIDER.forSpec(spec);

    assertThat(source).isNotNull().isInstanceOf(KafkaSource.class);
  }

  @Test
  void buildsSourceFromEarliestStartup() {
    var spec =
        KafkaIngressBuilder.forIdentifier(ID)
            .withKafkaAddress("localhost:9092")
            .withConsumerGroupId("g1")
            .withTopic("t1")
            .withDeserializer(NoopDeserializer.class)
            .withStartupPosition(KafkaIngressStartupPosition.fromEarliest())
            .build();

    assertThat(PROVIDER.forSpec(spec)).isNotNull();
  }

  @Test
  void buildsSourceFromLatestStartup() {
    var spec =
        KafkaIngressBuilder.forIdentifier(ID)
            .withKafkaAddress("localhost:9092")
            .withConsumerGroupId("g1")
            .withTopic("t1")
            .withDeserializer(NoopDeserializer.class)
            .withStartupPosition(KafkaIngressStartupPosition.fromLatest())
            .build();

    assertThat(PROVIDER.forSpec(spec)).isNotNull();
  }

  @Test
  void buildsSourceFromSpecificOffsetsStartup() {
    Map<KafkaTopicPartition, Long> offsets = new HashMap<>();
    offsets.put(new KafkaTopicPartition("t1", 0), 100L);
    offsets.put(new KafkaTopicPartition("t1", 1), 200L);
    offsets.put(new KafkaTopicPartition("t1", 2), 300L);

    var spec =
        KafkaIngressBuilder.forIdentifier(ID)
            .withKafkaAddress("localhost:9092")
            .withConsumerGroupId("g1")
            .withTopic("t1")
            .withDeserializer(NoopDeserializer.class)
            .withStartupPosition(KafkaIngressStartupPosition.fromSpecificOffsets(offsets))
            .build();

    assertThat(PROVIDER.forSpec(spec)).isNotNull();
  }

  @Test
  void buildsSourceFromDateStartup() {
    var spec =
        KafkaIngressBuilder.forIdentifier(ID)
            .withKafkaAddress("localhost:9092")
            .withConsumerGroupId("g1")
            .withTopic("t1")
            .withDeserializer(NoopDeserializer.class)
            .withStartupPosition(
                KafkaIngressStartupPosition.fromDate(
                    ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)))
            .build();

    assertThat(PROVIDER.forSpec(spec)).isNotNull();
  }

  @Test
  void wrongSpecTypeThrows() {
    IngressSpec<String> wrongSpec =
        new IngressSpec<>() {
          @Override
          public IngressIdentifier<String> id() {
            return ID;
          }

          @Override
          public IngressType type() {
            return new IngressType("wrong", "type");
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

  public static class NoopDeserializer implements KafkaIngressDeserializer<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> record) {
      return "";
    }
  }
}
