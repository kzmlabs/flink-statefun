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
    String topic = input.topic();
    byte[] key = requireNonNullKey(input);
    byte[] payload = requireNonNullValue(input);
    String id = new String(key, StandardCharsets.UTF_8);

    RoutingConfig routingConfig = routingConfigs.get(topic);
    if (routingConfig == null) {
      throw new IllegalStateException(
          "Consumed a record from topic [" + topic + "], but no routing config was specified.");
    }
    AutoRoutable.Builder routable =
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
    String key = header.key();
    byte[] value = header.value();
    Header.Builder proto = Header.newBuilder().setKey(key == null ? "" : key);
    if (value != null) {
      proto.setValue(MoreByteStrings.wrap(value)).setHasValue(true);
    }
    return proto.build();
  }

  private static byte[] requireNonNullKey(ConsumerRecord<byte[], byte[]> input) {
    byte[] key = input.key();
    if (key == null) {
      throw invalidRecord(input, "requires a UTF-8 key set for each record");
    }
    return key;
  }

  private static byte[] requireNonNullValue(ConsumerRecord<byte[], byte[]> input) {
    byte[] value = input.value();
    if (value == null) {
      throw invalidRecord(input, "cannot process a tombstone (null value) record");
    }
    return value;
  }

  /**
   * Builds the job-fatal rejection for an invalid record, carrying its topic, partition, offset,
   * timestamp and, when present, its key. Null-safe on every record field, so the diagnostic can
   * never itself throw regardless of which defect triggered it.
   */
  private static IllegalStateException invalidRecord(
      ConsumerRecord<byte[], byte[]> input, String defect) {
    TypeName tpe = RoutableKafkaIngressBinderV1.KIND_TYPE;
    byte[] key = input.key();
    String keySegment = key == null ? "" : ", key [" + new String(key, StandardCharsets.UTF_8) + "]";
    return new IllegalStateException(
        String.format(
            "The %s/%s ingress %s. Offending record: topic [%s], partition [%d], offset [%d], timestamp [%d]%s.",
            tpe.namespace(), tpe.name(), defect, input.topic(), input.partition(), input.offset(), input.timestamp(), keySegment));
  }
}
