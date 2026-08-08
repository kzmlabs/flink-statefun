// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import com.google.protobuf.Message;
import java.io.Serializable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Strategy applied by the routable ingress deserializer to an invalid record. Returning null drops
 * the record; returning a message delivers it instead of the original (a future forward strategy
 * will build a dead-letter envelope here); throwing fails the job. Serializable because handlers
 * ship to the cluster inside the deserializer. Implementations: SkipInvalidRecordHandler,
 * FailInvalidRecordHandler.
 */
interface InvalidRecordHandler extends Serializable {

  Message handle(ConsumerRecord<byte[], byte[]> record, InvalidRecordException.Defect defect);
}
