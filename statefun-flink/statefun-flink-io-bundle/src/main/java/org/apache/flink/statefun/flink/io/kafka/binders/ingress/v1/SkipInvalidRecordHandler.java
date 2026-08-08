// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import com.google.protobuf.Message;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drops the record and logs it individually with full coordinates, at the configured level. One
 * line per record by design: per-record diagnosability was chosen over flood protection, the
 * metrics remain the alerting signal. Runs only on the defect path, never on valid records.
 */
final class SkipInvalidRecordHandler implements InvalidRecordHandler {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(SkipInvalidRecordHandler.class);

  private final InvalidRecordPolicy.LogLevel logLevel;

  SkipInvalidRecordHandler(InvalidRecordPolicy.LogLevel logLevel) {
    this.logLevel = logLevel;
  }

  InvalidRecordPolicy.LogLevel logLevel() {
    return logLevel;
  }

  /** The message is built only when the target level is enabled, so a silenced poison flood costs no formatting. */
  @Override
  public Message handle(ConsumerRecord<byte[], byte[]> record, InvalidRecordException.Defect defect) {
    switch (logLevel) {
      case DEBUG -> { if (LOG.isDebugEnabled()) LOG.debug(message(record, defect)); }
      case INFO -> { if (LOG.isInfoEnabled()) LOG.info(message(record, defect)); }
      case WARN -> { if (LOG.isWarnEnabled()) LOG.warn(message(record, defect)); }
      case ERROR -> { if (LOG.isErrorEnabled()) LOG.error(message(record, defect)); }
    }
    return null;
  }

  private static String message(ConsumerRecord<byte[], byte[]> record, InvalidRecordException.Defect defect) {
    String key = record.key() == null ? "null" : new String(record.key(), StandardCharsets.UTF_8);
    int valueSize = record.value() == null ? -1 : record.value().length;
    return String.format("Skipping invalid record: defect [%s], %s, key [%s], value size [%d]", defect, InvalidRecordException.coordinates(record), key, valueSize);
  }
}
