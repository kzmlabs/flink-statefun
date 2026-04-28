// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.common.protobuf;

import java.io.IOException;
import java.io.OutputStream;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.core.memory.DataOutputView;

@NotThreadSafe
final class OutputStreamView extends OutputStream {
  private DataOutputView target;

  void set(DataOutputView target) {
    this.target = target;
  }

  void done() {
    target = null;
  }

  @Override
  public void write(@Nonnull byte[] b) throws IOException {
    target.write(b);
  }

  @Override
  public void write(@Nonnull byte[] b, int off, int len) throws IOException {
    target.write(b, off, len);
  }

  @Override
  public void write(int b) throws IOException {
    target.write(b);
  }

  @Override
  public void flush() {}

  @Override
  public void close() {}
}
