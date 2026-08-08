// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import com.google.protobuf.Message;
import java.io.Serializable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

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

  /** Drops the record; per-record logging arrives with the logging task. */
  final class Skip implements InvalidRecordHandler {
    private static final long serialVersionUID = 1L;

    private final InvalidRecordPolicy.LogLevel logLevel;

    Skip(InvalidRecordPolicy.LogLevel logLevel) {
      this.logLevel = logLevel;
    }

    InvalidRecordPolicy.LogLevel logLevel() {
      return logLevel;
    }

    @Override
    public Message handle(ConsumerRecord<byte[], byte[]> record, InvalidRecordException.Defect defect) {
      return null;
    }
  }
}
