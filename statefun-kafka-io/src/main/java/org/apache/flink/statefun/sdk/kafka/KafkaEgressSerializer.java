// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.kafka;

import java.io.Serializable;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * A {@link KafkaEgressSerializer} defines how to serialize values of type {@code T} into {@link
 * ProducerRecord ProducerRecords}.
 *
 * @param <OutT> the type of values being serialized
 */
public interface KafkaEgressSerializer<OutT> extends Serializable {

  /**
   * Serializes given element and returns it as a {@link ProducerRecord}.
   *
   * @param t element to be serialized
   * @return Kafka {@link ProducerRecord}
   */
  ProducerRecord<byte[], byte[]> serialize(OutT t);
}
