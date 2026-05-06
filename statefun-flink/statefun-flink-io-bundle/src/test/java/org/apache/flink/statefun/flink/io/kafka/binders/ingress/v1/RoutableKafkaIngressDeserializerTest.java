// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.Message;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.generated.TargetFunctionType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
        new RoutableKafkaIngressDeserializer(routingMap(ORDERS_TOPIC, ORDERS_ROUTING));
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

  // --- helpers ---------------------------------------------------------------------------------

  private static Map<String, RoutingConfig> routingMap(String key, RoutingConfig value) {
    final Map<String, RoutingConfig> map = new HashMap<>(1);
    map.put(key, value);
    return map;
  }

  private static ConsumerRecord<byte[], byte[]> consumerRecord(
      String topic, byte[] partitionKey, byte[] payload) {
    return new ConsumerRecord<>(topic, 0, 0L, partitionKey, payload);
  }
}
