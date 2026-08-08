// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import com.google.protobuf.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Job-fatal strict contract: every invalid record throws the pinned InvalidRecordException. */
final class FailInvalidRecordHandler implements InvalidRecordHandler {

  private static final long serialVersionUID = 1L;

  @Override
  public Message handle(ConsumerRecord<byte[], byte[]> record, InvalidRecordException.Defect defect) {
    throw InvalidRecordException.forRecord(record, defect);
  }
}
