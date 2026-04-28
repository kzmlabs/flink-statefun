// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.harness.io;

import java.util.Objects;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

final class ConsumingSink<T> implements Sink<T> {

  private static final long serialVersionUID = 1;

  private final SerializableConsumer<T> consumer;

  ConsumingSink(SerializableConsumer<T> consumer) {
    this.consumer = Objects.requireNonNull(consumer);
  }

  public SinkWriter<T> createWriter(WriterInitContext context) {
    return new ConsumingElementWriter();
  }

  private class ConsumingElementWriter implements SinkWriter<T> {
    public void write(T value, SinkWriter.Context context) {
      consumer.accept(value);
    }

    public void flush(boolean endOfInput) {}

    public void close() {}
  }
}
