// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import com.google.protobuf.Message;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Strategy applied by the routable ingress deserializer to an invalid record. Returning null drops
 * the record; returning a message delivers it instead of the original (the future forward strategy
 * will build a dead-letter envelope here); throwing fails the job. Serializable because handlers
 * ship to the cluster inside the deserializer.
 */
interface InvalidRecordHandler extends Serializable {

  Message handle(ConsumerRecord<byte[], byte[]> record, InvalidRecordException.Defect defect);

  /** Job-fatal strict contract: every invalid record throws the pinned InvalidRecordException. */
  final class Fail implements InvalidRecordHandler {
    private static final long serialVersionUID = 1L;

    @Override
    public Message handle(ConsumerRecord<byte[], byte[]> record, InvalidRecordException.Defect defect) {
      throw InvalidRecordException.forRecord(record, defect);
    }
  }

  /**
   * Drops the record and logs it individually with full coordinates, at the configured level. One
   * line per record by design: per-record diagnosability was chosen over flood protection, the
   * metrics remain the alerting signal.
   */
  final class Skip implements InvalidRecordHandler {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(Skip.class);

    private final InvalidRecordPolicy.LogLevel logLevel;

    Skip(InvalidRecordPolicy.LogLevel logLevel) {
      this.logLevel = logLevel;
    }

    InvalidRecordPolicy.LogLevel logLevel() {
      return logLevel;
    }

    @Override
    public Message handle(ConsumerRecord<byte[], byte[]> record, InvalidRecordException.Defect defect) {
      String key = record.key() == null ? "none" : new String(record.key(), StandardCharsets.UTF_8);
      int valueSize = record.value() == null ? -1 : record.value().length;
      String message = String.format(
          "Skipping invalid record: defect [%s], topic [%s], partition [%d], offset [%d], timestamp [%d], key [%s], value size [%d]",
          defect, record.topic(), record.partition(), record.offset(), record.timestamp(), key, valueSize);
      if (logLevel == InvalidRecordPolicy.LogLevel.ERROR) {
        LOG.error(message);
      } else {
        LOG.warn(message);
      }
      return null;
    }
  }
}
