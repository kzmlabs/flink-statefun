// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.common.protobuf;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.core.memory.DataInputView;

@NotThreadSafe
final class InputStreamView extends InputStream {
  private int limit;
  private DataInputView source;

  void set(DataInputView source, int serializedSize) {
    this.source = source;
    this.limit = serializedSize;
  }

  void done() {
    this.source = null;
    this.limit = 0;
  }

  @Override
  public int read() throws IOException {
    if (limit <= 0) {
      return -1;
    }
    --limit;
    return source.readByte();
  }

  @Override
  public int read(@Nonnull byte[] b, final int off, int len) throws IOException {
    if (limit <= 0) {
      return -1;
    }
    len = Math.min(len, limit);
    final int result = source.read(b, off, len);
    if (result >= 0) {
      limit -= result;
    }
    return result;
  }

  @Override
  public long skip(final long n) throws IOException {
    final int min = (int) Math.min(n, limit);
    final long result = source.skipBytes(min);
    if (result >= 0) {
      limit -= result;
    }
    return result;
  }

  @Override
  public synchronized void mark(int unused) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void reset() {
    throw new UnsupportedOperationException();
  }
}
