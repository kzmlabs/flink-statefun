// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.io.kinesis.binders.ingress.v1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.Message;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.generated.TargetFunctionType;
import org.apache.flink.statefun.sdk.kinesis.ingress.IngressRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for {@link RoutableKinesisIngressDeserializer}.
 *
 * <p>Covers the ARN-keyed routing path that {@link RoutableKinesisIngressSpec} configures for the
 * Flink 2.x {@code KinesisStreamsSource}: that source passes the stream <em>ARN</em> as the {@code
 * stream} argument of {@code KinesisDeserializationSchema.deserialize(...)}, which is then
 * surfaced as {@link IngressRecord#getStream()}. The deserializer must look up the routing config
 * by that exact value. A silent regression in Flink upstream (or in our wrapper) that flipped the
 * argument back to the short stream name would otherwise drop messages with no test failure.
 */
class RoutableKinesisIngressDeserializerTest {

  private static final String ORDERS_ARN =
      "arn:aws:kinesis:us-east-1:123456789012:stream/orders";
  private static final String EVENTS_ARN =
      "arn:aws:kinesis:us-east-1:123456789012:stream/events";

  private static final String ORDERS_VALUE_TYPE = "com.googleapis/com.mycomp.foo.OrderMessage";
  private static final String EVENTS_VALUE_TYPE = "com.googleapis/com.mycomp.foo.EventMessage";

  private static final TargetFunctionType ORDERS_TARGET =
      TargetFunctionType.newBuilder().setNamespace("com.mycomp.foo").setType("orders-fn").build();
  private static final TargetFunctionType EVENTS_TARGET_A =
      TargetFunctionType.newBuilder().setNamespace("com.mycomp.foo").setType("events-fn-a").build();
  private static final TargetFunctionType EVENTS_TARGET_B =
      TargetFunctionType.newBuilder().setNamespace("com.mycomp.foo").setType("events-fn-b").build();

  private static final RoutingConfig ORDERS_ROUTING =
      routingConfig(ORDERS_VALUE_TYPE, ORDERS_TARGET);

  private RoutableKinesisIngressDeserializer deserializer;

  @BeforeEach
  void setUp() {
    deserializer = new RoutableKinesisIngressDeserializer(routingMap(ORDERS_ARN, ORDERS_ROUTING));
  }

  @Test
  void routesArnKeyedRecordsToConfiguredTargets() {
    final byte[] payload = "order-123".getBytes(StandardCharsets.UTF_8);
    final IngressRecord record = ingressRecord(ORDERS_ARN, "pk-42", payload);

    final Message result = deserializer.deserialize(record);

    assertThat(result).isInstanceOf(AutoRoutable.class);
    final AutoRoutable routable = (AutoRoutable) result;
    assertThat(routable.getId()).isEqualTo("pk-42");
    assertThat(routable.getPayloadBytes().toByteArray()).isEqualTo(payload);
    assertThat(routable.getConfig().getTypeUrl()).isEqualTo(ORDERS_VALUE_TYPE);
    assertThat(routable.getConfig().getTargetFunctionTypesList()).containsExactly(ORDERS_TARGET);
  }

  /**
   * Regression guard: routing map is keyed by ARN (Flink 2.x contract). A short stream name or an
   * unconfigured ARN must both fail loudly so the regression is detected, rather than silently
   * dropping records.
   */
  @ParameterizedTest(name = "{1}")
  @MethodSource("nonMatchingStreamKeys")
  void throwsWhenStreamKeyIsNotInRoutingMap(String streamKey, String reason) {
    final IngressRecord record =
        ingressRecord(streamKey, "pk-1", "x".getBytes(StandardCharsets.UTF_8));

    assertThatThrownBy(() -> deserializer.deserialize(record))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(streamKey)
        .hasMessageContaining("no routing config");
  }

  static Stream<Arguments> nonMatchingStreamKeys() {
    return Stream.of(
        Arguments.of("orders", "short name (Flink 2.x regression guard)"),
        Arguments.of(EVENTS_ARN, "unconfigured ARN"));
  }

  /**
   * Empty Kinesis payloads are legal at the protocol level. The deserializer wraps the bytes in an
   * {@link AutoRoutable} envelope without parsing the payload itself (parsing is downstream), so
   * an empty payload should round-trip cleanly: success with zero-byte payload.
   */
  @Test
  void wrapsEmptyPayloadInAutoRoutableEnvelope() {
    final IngressRecord record = ingressRecord(ORDERS_ARN, "pk-empty", new byte[0]);

    final Message result = deserializer.deserialize(record);

    assertThat(result).isInstanceOf(AutoRoutable.class);
    final AutoRoutable routable = (AutoRoutable) result;
    assertThat(routable.getId()).isEqualTo("pk-empty");
    assertThat(routable.getPayloadBytes().size()).isZero();
    assertThat(routable.getConfig().getTypeUrl()).isEqualTo(ORDERS_VALUE_TYPE);
  }

  /**
   * Kinesis records cap at 1 MiB. The deserializer must not truncate. Uses a deterministic byte
   * pattern so we can verify exact bytes (including the boundary bytes) made it through.
   */
  @Test
  void preservesMaxSizePayloadWithoutTruncation() {
    final int oneMiB = 1024 * 1024;
    final byte[] payload = new byte[oneMiB];
    for (int i = 0; i < oneMiB; i++) {
      payload[i] = (byte) (i & 0xFF);
    }

    final IngressRecord record = ingressRecord(ORDERS_ARN, "pk-big", payload);

    final Message result = deserializer.deserialize(record);

    assertThat(result).isInstanceOf(AutoRoutable.class);
    final AutoRoutable routable = (AutoRoutable) result;
    assertThat(routable.getPayloadBytes().size()).isEqualTo(oneMiB);
    assertThat(routable.getPayloadBytes().toByteArray()).isEqualTo(payload);
  }

  /**
   * Catches accidental cross-stream state pollution: each ARN entry must dispatch independently to
   * its own configured value type / targets, even when the same deserializer instance handles
   * interleaved records from both streams.
   */
  @Test
  void dispatchesMultipleArnEntriesIndependently() {
    final RoutingConfig eventsRouting =
        routingConfig(EVENTS_VALUE_TYPE, EVENTS_TARGET_A, EVENTS_TARGET_B);

    final Map<String, RoutingConfig> routings = new HashMap<>();
    routings.put(ORDERS_ARN, ORDERS_ROUTING);
    routings.put(EVENTS_ARN, eventsRouting);

    final RoutableKinesisIngressDeserializer multiDeserializer =
        new RoutableKinesisIngressDeserializer(routings);

    final byte[] orderPayload = "order".getBytes(StandardCharsets.UTF_8);
    final byte[] eventPayloadFirst = "event-1".getBytes(StandardCharsets.UTF_8);
    final byte[] eventPayloadSecond = "event-2".getBytes(StandardCharsets.UTF_8);

    // Interleave the records to exercise both lookups within the same instance.
    final AutoRoutable orderResult =
        (AutoRoutable)
            multiDeserializer.deserialize(ingressRecord(ORDERS_ARN, "o-1", orderPayload));
    final AutoRoutable eventResult1 =
        (AutoRoutable)
            multiDeserializer.deserialize(ingressRecord(EVENTS_ARN, "e-1", eventPayloadFirst));
    final AutoRoutable orderResult2 =
        (AutoRoutable)
            multiDeserializer.deserialize(ingressRecord(ORDERS_ARN, "o-2", orderPayload));
    final AutoRoutable eventResult2 =
        (AutoRoutable)
            multiDeserializer.deserialize(ingressRecord(EVENTS_ARN, "e-2", eventPayloadSecond));

    // Orders side: orders value type and a single target.
    assertThat(orderResult.getId()).isEqualTo("o-1");
    assertThat(orderResult.getConfig().getTypeUrl()).isEqualTo(ORDERS_VALUE_TYPE);
    assertThat(orderResult.getConfig().getTargetFunctionTypesList()).containsExactly(ORDERS_TARGET);

    assertThat(orderResult2.getId()).isEqualTo("o-2");
    assertThat(orderResult2.getConfig().getTypeUrl()).isEqualTo(ORDERS_VALUE_TYPE);
    assertThat(orderResult2.getConfig().getTargetFunctionTypesList())
        .containsExactly(ORDERS_TARGET);

    // Events side: events value type and two distinct targets.
    assertThat(eventResult1.getId()).isEqualTo("e-1");
    assertThat(eventResult1.getConfig().getTypeUrl()).isEqualTo(EVENTS_VALUE_TYPE);
    assertThat(eventResult1.getConfig().getTargetFunctionTypesList())
        .containsExactly(EVENTS_TARGET_A, EVENTS_TARGET_B);
    assertThat(eventResult1.getPayloadBytes().toByteArray()).isEqualTo(eventPayloadFirst);

    assertThat(eventResult2.getId()).isEqualTo("e-2");
    assertThat(eventResult2.getConfig().getTypeUrl()).isEqualTo(EVENTS_VALUE_TYPE);
    assertThat(eventResult2.getConfig().getTargetFunctionTypesList())
        .containsExactly(EVENTS_TARGET_A, EVENTS_TARGET_B);
    assertThat(eventResult2.getPayloadBytes().toByteArray()).isEqualTo(eventPayloadSecond);
  }

  private static RoutingConfig routingConfig(String valueType, TargetFunctionType... targets) {
    final RoutingConfig.Builder builder = RoutingConfig.newBuilder().setTypeUrl(valueType);
    for (TargetFunctionType target : targets) {
      builder.addTargetFunctionTypes(target);
    }
    return builder.build();
  }

  private static Map<String, RoutingConfig> routingMap(String key, RoutingConfig value) {
    final Map<String, RoutingConfig> map = new HashMap<>(1);
    map.put(key, value);
    return map;
  }

  private static IngressRecord ingressRecord(String stream, String partitionKey, byte[] data) {
    return IngressRecord.newBuilder()
        .withStream(stream)
        .withShardId("shardId-000000000000")
        .withPartitionKey(partitionKey)
        .withSequenceNumber("seq-1")
        .withData(data)
        .withApproximateArrivalTimestamp(0L)
        .build();
  }
}
