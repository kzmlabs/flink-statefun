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
  private RoutableKafkaIngressDeserializer failPolicyDeserializer;

  @BeforeEach
  void setUp() {
    deserializer =
        new RoutableKafkaIngressDeserializer(
            routingMap(ORDERS_TOPIC, ORDERS_ROUTING), Set.of(ORDERS_TOPIC), Map.of());
    failPolicyDeserializer =
        new RoutableKafkaIngressDeserializer(
            routingMap(ORDERS_TOPIC, ORDERS_ROUTING), Set.of(ORDERS_TOPIC), Map.of(ORDERS_TOPIC, InvalidRecordPolicy.fail()));
  }

  @Test
  void routesConfiguredTopicToTargets() {
    byte[] payload = "order-payload".getBytes(StandardCharsets.UTF_8);
    byte[] key = "pk-7".getBytes(StandardCharsets.UTF_8);
    ConsumerRecord<byte[], byte[]> record = consumerRecord(ORDERS_TOPIC, key, payload);

    Message result = deserializer.deserialize(record);

    assertThat(result).isInstanceOf(AutoRoutable.class);
    AutoRoutable routable = (AutoRoutable) result;
    assertThat(routable.getId()).isEqualTo("pk-7");
    assertThat(routable.getPayloadBytes().toByteArray()).isEqualTo(payload);
    assertThat(routable.getConfig().getTypeUrl()).isEqualTo(ORDERS_VALUE_TYPE);
    assertThat(routable.getConfig().getTargetFunctionTypesList()).containsExactly(ORDERS_TARGET);
  }

  @Test
  void capturesRecordHeadersInOrderIncludingNullValues() {
    ConsumerRecord<byte[], byte[]> record =
        consumerRecord(
            ORDERS_TOPIC,
            "pk-7".getBytes(StandardCharsets.UTF_8),
            "p".getBytes(StandardCharsets.UTF_8));
    record.headers().add("trace-id", "abc-123".getBytes(StandardCharsets.UTF_8));
    record.headers().add("empty-header", null);
    record.headers().add("trace-id", "def-456".getBytes(StandardCharsets.UTF_8));

    AutoRoutable routable = (AutoRoutable) deserializer.deserialize(record);

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
    ConsumerRecord<byte[], byte[]> record =
        consumerRecord(
            ORDERS_TOPIC,
            "pk".getBytes(StandardCharsets.UTF_8),
            "p".getBytes(StandardCharsets.UTF_8));

    AutoRoutable routable = (AutoRoutable) deserializer.deserialize(record);

    assertThat(routable.getHeadersCount()).isZero();
  }

  @Test
  void headersAreIgnoredWhenTopicHasNotOptedIntoForwarding() {
    RoutableKafkaIngressDeserializer optedOut =
        new RoutableKafkaIngressDeserializer(routingMap(ORDERS_TOPIC, ORDERS_ROUTING), Set.of(), Map.of());
    ConsumerRecord<byte[], byte[]> record =
        consumerRecord(
            ORDERS_TOPIC,
            "pk-7".getBytes(StandardCharsets.UTF_8),
            "p".getBytes(StandardCharsets.UTF_8));
    record.headers().add("trace-id", "abc-123".getBytes(StandardCharsets.UTF_8));

    AutoRoutable routable = (AutoRoutable) optedOut.deserialize(record);

    assertThat(routable.getHeadersCount()).isZero();
  }

  @Test
  void emptyKeyIsAValidAddressRoutingToEmptyInstanceId() {
    ConsumerRecord<byte[], byte[]> record = consumerRecord(ORDERS_TOPIC, new byte[0], "p".getBytes(StandardCharsets.UTF_8));

    AutoRoutable routable = (AutoRoutable) deserializer.deserialize(record);

    assertThat(routable.getId()).isEmpty();
  }

  @Test
  void skipLogsEveryRecordWithFullContextAtWarnByDefault() {
    ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> log = captureSkipLog();

    deserializer.deserialize(consumerRecordAt(ORDERS_TOPIC, 3, 42L, 1690000000123L, null, "x".getBytes(StandardCharsets.UTF_8)));
    deserializer.deserialize(consumerRecordAt(ORDERS_TOPIC, 1, 7L, 1690000000456L, "pk-7".getBytes(StandardCharsets.UTF_8), null));

    assertThat(log.list).hasSize(2);
    assertThat(log.list.get(0).getLevel()).isEqualTo(ch.qos.logback.classic.Level.WARN);
    assertThat(log.list.get(0).getFormattedMessage())
        .contains("defect [NULL_KEY]")
        .contains("topic [" + ORDERS_TOPIC + "]")
        .contains("partition [3]")
        .contains("offset [42]")
        .contains("timestamp [1690000000123]")
        .contains("key [null]")
        .contains("value size [1]");
    assertThat(log.list.get(1).getLevel()).isEqualTo(ch.qos.logback.classic.Level.WARN);
    assertThat(log.list.get(1).getFormattedMessage())
        .contains("defect [NULL_VALUE]")
        .contains("partition [1]")
        .contains("offset [7]")
        .contains("key [pk-7]")
        .contains("value size [-1]");
  }

  @Test
  void skipLogsAtEveryConfigurableLevel() {
    ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> log = captureSkipLog();
    Map<InvalidRecordPolicy.LogLevel, ch.qos.logback.classic.Level> expected = Map.of(
        InvalidRecordPolicy.LogLevel.DEBUG, ch.qos.logback.classic.Level.DEBUG,
        InvalidRecordPolicy.LogLevel.INFO, ch.qos.logback.classic.Level.INFO,
        InvalidRecordPolicy.LogLevel.WARN, ch.qos.logback.classic.Level.WARN,
        InvalidRecordPolicy.LogLevel.ERROR, ch.qos.logback.classic.Level.ERROR);

    for (Map.Entry<InvalidRecordPolicy.LogLevel, ch.qos.logback.classic.Level> entry : expected.entrySet()) {
      log.list.clear();
      RoutableKafkaIngressDeserializer configured =
          new RoutableKafkaIngressDeserializer(
              routingMap(ORDERS_TOPIC, ORDERS_ROUTING), Set.of(), Map.of(ORDERS_TOPIC, InvalidRecordPolicy.skip(entry.getKey())));

      configured.deserialize(consumerRecordAt(ORDERS_TOPIC, 0, 5L, 1690000000789L, null, "x".getBytes(StandardCharsets.UTF_8)));

      assertThat(log.list).as("one log event at level %s", entry.getKey()).hasSize(1);
      assertThat(log.list.get(0).getLevel()).isEqualTo(entry.getValue());
    }
  }

  private static ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> captureSkipLog() {
    ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(SkipInvalidRecordHandler.class);
    logger.setLevel(ch.qos.logback.classic.Level.ALL);
    logger.detachAndStopAllAppenders();
    ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender = new ch.qos.logback.core.read.ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    return appender;
  }

  @Test
  void skippedRecordsAreCountedByTopicAndDefect() {
    org.apache.flink.statefun.flink.io.testutils.RecordingMetricGroup metrics = new org.apache.flink.statefun.flink.io.testutils.RecordingMetricGroup();
    deserializer.registerInvalidRecordMetrics(metrics.group());

    deserializer.deserialize(consumerRecordAt(ORDERS_TOPIC, 0, 1L, 1690000000100L, null, "x".getBytes(StandardCharsets.UTF_8)));
    deserializer.deserialize(consumerRecordAt(ORDERS_TOPIC, 0, 2L, 1690000000200L, "pk-1".getBytes(StandardCharsets.UTF_8), null));
    deserializer.deserialize(consumerRecordAt(ORDERS_TOPIC, 0, 3L, 1690000000300L, "pk-2".getBytes(StandardCharsets.UTF_8), null));

    assertThat(metrics.count("topic." + ORDERS_TOPIC + ".defect.NULL_KEY.numInvalidRecordsSkipped")).isEqualTo(1);
    assertThat(metrics.count("topic." + ORDERS_TOPIC + ".defect.NULL_VALUE.numInvalidRecordsSkipped")).isEqualTo(2);
  }

  @Test
  void skippingWithoutRegisteredMetricsIsANoOp() {
    assertThat(deserializer.deserialize(consumerRecordAt(ORDERS_TOPIC, 0, 1L, 1690000000100L, null, "x".getBytes(StandardCharsets.UTF_8)))).isNull();
  }

  @Test
  void skipPolicyIsTheDefaultAndDropsNullKeyRecords() {
    ConsumerRecord<byte[], byte[]> record = consumerRecordAt(ORDERS_TOPIC, 3, 42L, 1690000000123L, null, "x".getBytes(StandardCharsets.UTF_8));

    assertThat(deserializer.deserialize(record)).isNull();
  }

  @Test
  void skipPolicyIsTheDefaultAndDropsTombstones() {
    ConsumerRecord<byte[], byte[]> record = consumerRecordAt(ORDERS_TOPIC, 1, 7L, 1690000000456L, "pk-7".getBytes(StandardCharsets.UTF_8), null);

    assertThat(deserializer.deserialize(record)).isNull();
  }

  @Test
  void nullKeyFailureReportsRecordCoordinates() {
    ConsumerRecord<byte[], byte[]> record = consumerRecordAt(ORDERS_TOPIC, 3, 42L, 1690000000123L, null, "x".getBytes(StandardCharsets.UTF_8));

    assertThatThrownBy(() -> failPolicyDeserializer.deserialize(record))
        .isInstanceOf(InvalidRecordException.class)
        .hasMessageContaining("requires a UTF-8 key")
        .hasMessageContaining("topic [" + ORDERS_TOPIC + "]")
        .hasMessageContaining("partition [3]")
        .hasMessageContaining("offset [42]")
        .hasMessageContaining("timestamp [1690000000123]");
  }

  @Test
  void tombstoneFailureReportsRecordCoordinatesAndKey() {
    ConsumerRecord<byte[], byte[]> record = consumerRecordAt(ORDERS_TOPIC, 1, 7L, 1690000000456L, "pk-7".getBytes(StandardCharsets.UTF_8), null);

    assertThatThrownBy(() -> failPolicyDeserializer.deserialize(record))
        .isInstanceOf(InvalidRecordException.class)
        .hasMessageContaining("tombstone")
        .hasMessageContaining("topic [" + ORDERS_TOPIC + "]")
        .hasMessageContaining("partition [1]")
        .hasMessageContaining("offset [7]")
        .hasMessageContaining("timestamp [1690000000456]")
        .hasMessageContaining("key [pk-7]");
  }

  @Test
  void throwsWhenTopicIsNotInRoutingMap() {
    ConsumerRecord<byte[], byte[]> record =
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
    Map<String, RoutingConfig> map = new HashMap<>(1);
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
