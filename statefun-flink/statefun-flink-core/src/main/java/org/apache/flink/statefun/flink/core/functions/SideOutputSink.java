// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.functions;

import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

final class SideOutputSink {
  private final Map<EgressIdentifier<?>, OutputTag<Object>> outputTags;
  private final Output<?> output;
  private final StreamRecord<Object> record;

  SideOutputSink(Map<EgressIdentifier<?>, OutputTag<Object>> outputTags, Output<?> output) {
    this.outputTags = Objects.requireNonNull(outputTags);
    this.output = Objects.requireNonNull(output);
    this.record = new StreamRecord<>(null);
  }

  <T> void accept(EgressIdentifier<T> id, T message) {
    Objects.requireNonNull(id);
    Objects.requireNonNull(message);

    OutputTag<Object> tag = outputTags.get(id);
    if (tag == null) {
      throw new IllegalArgumentException("Unknown egress " + id);
    }
    record.replace(message);
    output.collect(tag, record);
  }
}
