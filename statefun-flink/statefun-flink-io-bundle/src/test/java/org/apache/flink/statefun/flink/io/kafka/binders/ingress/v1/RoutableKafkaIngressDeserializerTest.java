// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.Message;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.generated.TargetFunctionType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Lifts {@link RoutableKafkaIngressDeserializer} off zero unit-test coverage with a happy-path
 * routing test plus a missing-topic regression guard. The Kafka path is keyed by topic name (not
 * ARN), so the failure modes differ from the Kinesis side, but the envelope-building contract is
 * the same.
 */
class RoutableKafkaIngressDeserializerTest {

  private static final String ORDERS_TOPIC = "orders";
  private static final String ORDERS_VALUE_TYPE = "com.googleapis/com.mycomp.foo.OrderMessage";
  private static final TargetFunctionType ORDERS_TARGET =
      TargetFunctionType.newBuilder().setNamespace("com.mycomp.foo").setType("orders-fn").build();
  private static final RoutingConfig ORDERS_ROUTING =
      RoutingConfig.newBuilder()
          .setTypeUrl(ORDERS_VALUE_TYPE)
          .addTargetFunctionTypes(ORDERS_TARGET)
          .build();

  private RoutableKafkaIngressDeserializer deserializer;

  @BeforeEach
  void setUp() {
    deserializer =
        new RoutableKafkaIngressDeserializer(
            routingMap(ORDERS_TOPIC, ORDERS_ROUTING), Set.of(ORDERS_TOPIC));
  }

  @Test
  void routesConfiguredTopicToTargets() {
    final byte[] payload = "order-payload".getBytes(StandardCharsets.UTF_8);
    final byte[] key = "pk-7".getBytes(StandardCharsets.UTF_8);
    final ConsumerRecord<byte[], byte[]> record = consumerRecord(ORDERS_TOPIC, key, payload);

    final Message result = deserializer.deserialize(record);

    assertThat(result).isInstanceOf(AutoRoutable.class);
    final AutoRoutable routable = (AutoRoutable) result;
    assertThat(routable.getId()).isEqualTo("pk-7");
    assertThat(routable.getPayloadBytes().toByteArray()).isEqualTo(payload);
    assertThat(routable.getConfig().getTypeUrl()).isEqualTo(ORDERS_VALUE_TYPE);
    assertThat(routable.getConfig().getTargetFunctionTypesList()).containsExactly(ORDERS_TARGET);
  }

  @Test
  void capturesRecordHeadersInOrderIncludingNullValues() {
    final ConsumerRecord<byte[], byte[]> record =
        consumerRecord(
            ORDERS_TOPIC,
            "pk-7".getBytes(StandardCharsets.UTF_8),
            "p".getBytes(StandardCharsets.UTF_8));
    record.headers().add("trace-id", "abc-123".getBytes(StandardCharsets.UTF_8));
    record.headers().add("empty-header", null);
    record.headers().add("trace-id", "def-456".getBytes(StandardCharsets.UTF_8));

    final AutoRoutable routable = (AutoRoutable) deserializer.deserialize(record);

    assertThat(routable.getHeadersCount()).isEqualTo(3);
    assertThat(routable.getHeaders(0).getKey()).isEqualTo("trace-id");
    assertThat(routable.getHeaders(0).getHasValue()).isTrue();
    assertThat(routable.getHeaders(0).getValue().toStringUtf8()).isEqualTo("abc-123");
    assertThat(routable.getHeaders(1).getKey()).isEqualTo("empty-header");
    assertThat(routable.getHeaders(1).getHasValue()).isFalse();
    assertThat(routable.getHeaders(1).getValue().isEmpty()).isTrue();
    assertThat(routable.getHeaders(2).getKey()).isEqualTo("trace-id");
    assertThat(routable.getHeaders(2).getHasValue()).isTrue();
    assertThat(routable.getHeaders(2).getValue().toStringUtf8()).isEqualTo("def-456");
  }

  @Test
  void recordWithoutHeadersProducesEmptyHeaderList() {
    final ConsumerRecord<byte[], byte[]> record =
        consumerRecord(
            ORDERS_TOPIC,
            "pk".getBytes(StandardCharsets.UTF_8),
            "p".getBytes(StandardCharsets.UTF_8));

    final AutoRoutable routable = (AutoRoutable) deserializer.deserialize(record);

    assertThat(routable.getHeadersCount()).isZero();
  }

  @Test
  void headersAreIgnoredWhenTopicHasNotOptedIntoForwarding() {
    final RoutableKafkaIngressDeserializer optedOut =
        new RoutableKafkaIngressDeserializer(routingMap(ORDERS_TOPIC, ORDERS_ROUTING), Set.of());
    final ConsumerRecord<byte[], byte[]> record =
        consumerRecord(
            ORDERS_TOPIC,
            "pk-7".getBytes(StandardCharsets.UTF_8),
            "p".getBytes(StandardCharsets.UTF_8));
    record.headers().add("trace-id", "abc-123".getBytes(StandardCharsets.UTF_8));

    final AutoRoutable routable = (AutoRoutable) optedOut.deserialize(record);

    assertThat(routable.getHeadersCount()).isZero();
  }

  @Test
  void emptyKeyIsAValidAddressRoutingToEmptyInstanceId() {
    final ConsumerRecord<byte[], byte[]> record =
        consumerRecord(ORDERS_TOPIC, new byte[0], "p".getBytes(StandardCharsets.UTF_8));

    final AutoRoutable routable = (AutoRoutable) deserializer.deserialize(record);

    assertThat(routable.getId()).isEmpty();
  }

  @Test
  void nullKeyFailureReportsRecordCoordinates() {
    final ConsumerRecord<byte[], byte[]> record =
        consumerRecordAt(
            ORDERS_TOPIC, 3, 42L, 1690000000123L, null, "x".getBytes(StandardCharsets.UTF_8));

    assertThatThrownBy(() -> deserializer.deserialize(record))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("requires a UTF-8 key")
        .hasMessageContaining("topic [" + ORDERS_TOPIC + "]")
        .hasMessageContaining("partition [3]")
        .hasMessageContaining("offset [42]")
        .hasMessageContaining("timestamp [1690000000123]");
  }

  @Test
  void tombstoneFailureReportsRecordCoordinatesAndKey() {
    final ConsumerRecord<byte[], byte[]> record =
        consumerRecordAt(
            ORDERS_TOPIC, 1, 7L, 1690000000456L, "pk-7".getBytes(StandardCharsets.UTF_8), null);

    assertThatThrownBy(() -> deserializer.deserialize(record))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("tombstone")
        .hasMessageContaining("topic [" + ORDERS_TOPIC + "]")
        .hasMessageContaining("partition [1]")
        .hasMessageContaining("offset [7]")
        .hasMessageContaining("timestamp [1690000000456]")
        .hasMessageContaining("key [pk-7]");
  }

  @Test
  void throwsWhenTopicIsNotInRoutingMap() {
    final ConsumerRecord<byte[], byte[]> record =
        consumerRecord(
            "unknown-topic",
            "pk".getBytes(StandardCharsets.UTF_8),
            "x".getBytes(StandardCharsets.UTF_8));

    assertThatThrownBy(() -> deserializer.deserialize(record))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("unknown-topic")
        .hasMessageContaining("no routing config");
  }

  private static Map<String, RoutingConfig> routingMap(String key, RoutingConfig value) {
    final Map<String, RoutingConfig> map = new HashMap<>(1);
    map.put(key, value);
    return map;
  }

  private static ConsumerRecord<byte[], byte[]> consumerRecord(
      String topic, byte[] partitionKey, byte[] payload) {
    return new ConsumerRecord<>(topic, 0, 0L, partitionKey, payload);
  }

  private static ConsumerRecord<byte[], byte[]> consumerRecordAt(
      String topic, int partition, long offset, long timestamp, byte[] key, byte[] payload) {
    return new ConsumerRecord<>(
        topic,
        partition,
        offset,
        timestamp,
        TimestampType.CREATE_TIME,
        key == null ? -1 : key.length,
        payload == null ? -1 : payload.length,
        key,
        payload,
        new RecordHeaders(),
        Optional.empty());
  }
}
