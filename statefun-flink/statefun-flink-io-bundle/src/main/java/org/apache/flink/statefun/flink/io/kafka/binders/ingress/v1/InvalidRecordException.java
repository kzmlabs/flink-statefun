// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import java.nio.charset.StandardCharsets;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Job-fatal rejection of an invalid Kafka record under the fail policy. Extends
 * IllegalStateException so the message contract established before the policy existed stays intact
 * for log-grepping tooling; the defect adds a typed classification on top.
 */
final class InvalidRecordException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  enum Defect {
    NULL_KEY,
    NULL_VALUE
  }

  private final Defect defect;

  InvalidRecordException(Defect defect, String message) {
    super(message);
    this.defect = defect;
  }

  /**
   * Builds the job-fatal rejection for an invalid record, carrying its topic, partition, offset,
   * timestamp and, when present, its key. Null-safe on every record field, so the diagnostic can
   * never itself throw regardless of which defect triggered it. The message wording predates the
   * skip policy and is a pinned contract for log-grepping tooling.
   */
  static InvalidRecordException forRecord(ConsumerRecord<byte[], byte[]> input, Defect defect) {
    TypeName tpe = RoutableKafkaIngressBinderV1.KIND_TYPE;
    byte[] key = input.key();
    String keySegment = key == null ? "" : ", key [" + new String(key, StandardCharsets.UTF_8) + "]";
    String reason = defect == Defect.NULL_KEY ? "requires a UTF-8 key set for each record" : "cannot process a tombstone (null value) record";
    return new InvalidRecordException(
        defect,
        String.format(
            "The %s/%s ingress %s. Offending record: topic [%s], partition [%d], offset [%d], timestamp [%d]%s.",
            tpe.namespace(), tpe.name(), reason, input.topic(), input.partition(), input.offset(), input.timestamp(), keySegment));
  }

  Defect defect() {
    return defect;
  }
}
