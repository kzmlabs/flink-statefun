// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.kafka;

import java.io.Serializable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The deserialization schema describes how to turn the Kafka ConsumerRecords into data types that
 * are processed by the system.
 *
 * @param <T> The type created by the keyed deserialization schema.
 */
public interface KafkaIngressDeserializer<T> extends Serializable {

  /**
   * Deserializes the Kafka record.
   *
   * @param input Kafka record to be deserialized.
   * @return The deserialized message as an object (null if the message cannot be deserialized).
   */
  T deserialize(ConsumerRecord<byte[], byte[]> input);
}
