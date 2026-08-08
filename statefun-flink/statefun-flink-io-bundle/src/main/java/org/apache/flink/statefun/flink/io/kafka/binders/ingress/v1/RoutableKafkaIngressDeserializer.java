// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import com.google.protobuf.Message;
import com.google.protobuf.MoreByteStrings;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.statefun.flink.io.generated.Header;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class RoutableKafkaIngressDeserializer
    implements org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer<Message> {

  private static final long serialVersionUID = 1L;

  private final Map<String, RoutingConfig> routingConfigs;
  private final Set<String> forwardHeaderTopics;

  public RoutableKafkaIngressDeserializer(
      Map<String, RoutingConfig> routingConfigs, Set<String> forwardHeaderTopics) {
    if (routingConfigs == null || routingConfigs.isEmpty()) {
      throw new IllegalArgumentException(
          "Routing config for routable Kafka ingress cannot be empty.");
    }
    this.routingConfigs = routingConfigs;
    this.forwardHeaderTopics =
        forwardHeaderTopics == null ? Set.of() : Set.copyOf(forwardHeaderTopics);
  }

  @Override
  public Message deserialize(ConsumerRecord<byte[], byte[]> input) {
    final String topic = input.topic();
    final byte[] key = requireNonNullKey(input);
    final byte[] payload = requireNonNullValue(input);
    final String id = new String(key, StandardCharsets.UTF_8);

    final RoutingConfig routingConfig = routingConfigs.get(topic);
    if (routingConfig == null) {
      throw new IllegalStateException(
          "Consumed a record from topic [" + topic + "], but no routing config was specified.");
    }
    final AutoRoutable.Builder routable =
        AutoRoutable.newBuilder()
            .setConfig(routingConfig)
            .setId(id)
            .setPayloadBytes(MoreByteStrings.wrap(payload));
    // headers are captured only for topics that opted in via forwardHeaders; the second
    // guard keeps the per-record hot path allocation-free for the common header-less case
    if (forwardHeaderTopics.contains(topic) && input.headers().iterator().hasNext()) {
      routable.addAllHeaders(
          StreamSupport.stream(input.headers().spliterator(), false)
              .map(RoutableKafkaIngressDeserializer::toProtoHeader)
              .toList());
    }
    return routable.build();
  }

  private static Header toProtoHeader(org.apache.kafka.common.header.Header header) {
    final String key = header.key();
    final byte[] value = header.value();
    final Header.Builder proto = Header.newBuilder().setKey(key == null ? "" : key);
    if (value != null) {
      proto.setValue(MoreByteStrings.wrap(value)).setHasValue(true);
    }
    return proto.build();
  }

  private static byte[] requireNonNullKey(ConsumerRecord<byte[], byte[]> input) {
    final byte[] key = input.key();
    if (key == null) {
      TypeName tpe = RoutableKafkaIngressBinderV1.KIND_TYPE;
      throw new IllegalStateException(
          "The "
              + tpe.namespace()
              + "/"
              + tpe.name()
              + " ingress requires a UTF-8 key set for each record. Offending record: "
              + recordCoordinates(input)
              + ".");
    }
    return key;
  }

  // runs after the key check, so input.key() is known to be non-null here
  private static byte[] requireNonNullValue(ConsumerRecord<byte[], byte[]> input) {
    final byte[] value = input.value();
    if (value == null) {
      TypeName tpe = RoutableKafkaIngressBinderV1.KIND_TYPE;
      throw new IllegalStateException(
          "The "
              + tpe.namespace()
              + "/"
              + tpe.name()
              + " ingress cannot process a tombstone (null value) record. Offending record: "
              + recordCoordinates(input)
              + ", key ["
              + new String(input.key(), StandardCharsets.UTF_8)
              + "].");
    }
    return value;
  }

  private static String recordCoordinates(ConsumerRecord<byte[], byte[]> input) {
    return "topic ["
        + input.topic()
        + "], partition ["
        + input.partition()
        + "], offset ["
        + input.offset()
        + "], timestamp ["
        + input.timestamp()
        + "]";
  }
}
