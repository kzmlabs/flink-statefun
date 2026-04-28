// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.functions;

import java.util.Objects;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

final class RemoteSink {
  private final Output<StreamRecord<Message>> output;
  private final StreamRecord<Message> record;

  RemoteSink(Output<StreamRecord<Message>> output) {
    this.output = Objects.requireNonNull(output);
    this.record = new StreamRecord<>(null);
  }

  void accept(Message envelope) {
    Objects.requireNonNull(envelope);
    output.collect(record.replace(envelope));
  }
}
