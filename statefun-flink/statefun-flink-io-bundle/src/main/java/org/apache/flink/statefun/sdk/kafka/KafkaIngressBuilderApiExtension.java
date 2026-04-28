// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.kafka;

public class KafkaIngressBuilderApiExtension {
  public static <T> void withDeserializer(
      KafkaIngressBuilder<T> kafkaIngressBuilder, KafkaIngressDeserializer<T> deserializer) {
    kafkaIngressBuilder.withDeserializer(deserializer);
  }
}
