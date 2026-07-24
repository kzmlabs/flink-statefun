// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import com.google.protobuf.Message;
import com.google.protobuf.MoreByteStrings;
import java.nio.charset.StandardCharsets;
import java.util.Map;
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

  public RoutableKafkaIngressDeserializer(Map<String, RoutingConfig> routingConfigs) {
    if (routingConfigs == null || routingConfigs.isEmpty()) {
      throw new IllegalArgumentException(
          "Routing config for routable Kafka ingress cannot be empty.");
    }
    this.routingConfigs = routingConfigs;
  }

  @Override
  public Message deserialize(ConsumerRecord<byte[], byte[]> input) {
    final String topic = input.topic();
    final byte[] payload = input.value();
    final byte[] key = requireNonNullKey(input.key());
    final String id = new String(key, StandardCharsets.UTF_8);

    final RoutingConfig routingConfig = routingConfigs.get(topic);
    if (routingConfig == null) {
      throw new IllegalStateException(
          "Consumed a record from topic [" + topic + "], but no routing config was specified.");
    }
    return AutoRoutable.newBuilder()
        .setConfig(routingConfig)
        .setId(id)
        .setPayloadBytes(MoreByteStrings.wrap(payload))
        .addAllHeaders(
            StreamSupport.stream(input.headers().spliterator(), false)
                .map(RoutableKafkaIngressDeserializer::toProtoHeader)
                .toList())
        .build();
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

  private byte[] requireNonNullKey(byte[] key) {
    if (key == null) {
      TypeName tpe = RoutableKafkaIngressBinderV1.KIND_TYPE;
      throw new IllegalStateException(
          "The "
              + tpe.namespace()
              + "/"
              + tpe.name()
              + " ingress requires a UTF-8 key set for each record.");
    }
    return key;
  }
}
