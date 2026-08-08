// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import com.google.protobuf.Message;
import com.google.protobuf.MoreByteStrings;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.statefun.flink.io.generated.Header;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class RoutableKafkaIngressDeserializer
    implements org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer<Message> {

  private static final long serialVersionUID = 1L;

  private static final InvalidRecordHandler DEFAULT_HANDLER = InvalidRecordPolicy.defaults().handler();

  private final Map<String, RoutingConfig> routingConfigs;
  private final Set<String> forwardHeaderTopics;
  private final Map<String, InvalidRecordHandler> invalidRecordHandlerByTopic;

  public RoutableKafkaIngressDeserializer(
      Map<String, RoutingConfig> routingConfigs, Set<String> forwardHeaderTopics, Map<String, InvalidRecordPolicy> invalidRecordPolicyByTopic) {
    if (routingConfigs == null || routingConfigs.isEmpty()) {
      throw new IllegalArgumentException(
          "Routing config for routable Kafka ingress cannot be empty.");
    }
    this.routingConfigs = routingConfigs;
    this.forwardHeaderTopics =
        forwardHeaderTopics == null ? Set.of() : Set.copyOf(forwardHeaderTopics);
    this.invalidRecordHandlerByTopic =
        invalidRecordPolicyByTopic == null
            ? Map.of()
            : invalidRecordPolicyByTopic.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().handler()));
  }

  /** Returns the routable envelope, or null when the record is invalid and its topic's policy is skip. */
  @Override
  public Message deserialize(ConsumerRecord<byte[], byte[]> input) {
    String topic = input.topic();
    if (input.key() == null) {
      return handlerFor(topic).handle(input, InvalidRecordException.Defect.NULL_KEY);
    }
    if (input.value() == null) {
      return handlerFor(topic).handle(input, InvalidRecordException.Defect.NULL_VALUE);
    }
    byte[] key = input.key();
    byte[] payload = input.value();
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

  private InvalidRecordHandler handlerFor(String topic) {
    return invalidRecordHandlerByTopic.getOrDefault(topic, DEFAULT_HANDLER);
  }
}
