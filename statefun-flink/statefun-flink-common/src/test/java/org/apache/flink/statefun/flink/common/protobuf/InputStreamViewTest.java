// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.common.protobuf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.junit.jupiter.api.Test;

class InputStreamViewTest {

  @Test
  void singleByteReadHonorsLimit() throws Exception {
    InputStreamView view = new InputStreamView();
    view.set(viewOver(new byte[] {1, 2, 3, 4, 5}), 3);

    assertThat(view.read()).isEqualTo(1);
    assertThat(view.read()).isEqualTo(2);
    assertThat(view.read()).isEqualTo(3);
    // Limit reached even though there are more bytes in the underlying stream.
    assertThat(view.read()).isEqualTo(-1);
  }

  @Test
  void readIntoBufferStopsAtLimit() throws Exception {
    InputStreamView view = new InputStreamView();
    view.set(viewOver(new byte[] {10, 20, 30, 40, 50}), 3);

    byte[] buf = new byte[10];
    int n = view.read(buf, 0, 10);

    assertThat(n).isEqualTo(3);
    assertThat(buf[0]).isEqualTo((byte) 10);
    assertThat(buf[1]).isEqualTo((byte) 20);
    assertThat(buf[2]).isEqualTo((byte) 30);
    // After hitting the limit, subsequent reads return -1.
    assertThat(view.read(buf, 0, 10)).isEqualTo(-1);
  }

  @Test
  void readIntoBufferAccountsForPartialReads() throws Exception {
    InputStreamView view = new InputStreamView();
    view.set(viewOver(new byte[] {1, 2, 3, 4, 5}), 5);

    byte[] buf = new byte[2];
    assertThat(view.read(buf, 0, 2)).isEqualTo(2);
    assertThat(view.read(buf, 0, 2)).isEqualTo(2);
    // 1 byte left under the limit.
    assertThat(view.read(buf, 0, 2)).isEqualTo(1);
    // Limit exhausted.
    assertThat(view.read(buf, 0, 2)).isEqualTo(-1);
  }

  @Test
  void skipDecrementsLimit() throws Exception {
    InputStreamView view = new InputStreamView();
    view.set(viewOver(new byte[] {1, 2, 3, 4, 5}), 5);

    long skipped = view.skip(3);

    assertThat(skipped).isEqualTo(3);
    assertThat(view.read()).isEqualTo(4);
    assertThat(view.read()).isEqualTo(5);
    assertThat(view.read()).isEqualTo(-1);
  }

  @Test
  void skipIsBoundedByLimit() throws Exception {
    InputStreamView view = new InputStreamView();
    view.set(viewOver(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}), 4);

    long skipped = view.skip(100);

    assertThat(skipped).isEqualTo(4);
    assertThat(view.read()).isEqualTo(-1);
  }

  @Test
  void doneResetsForReuse() throws Exception {
    InputStreamView view = new InputStreamView();
    view.set(viewOver(new byte[] {1, 2, 3}), 3);

    view.done();

    // After done, no more bytes are readable. The view is now ready to be reused with a
    // fresh `set(...)` call — this is the InputStreamView's contract for being pooled.
    assertThat(view.read()).isEqualTo(-1);
  }

  @Test
  void markAndResetAreUnsupported() {
    InputStreamView view = new InputStreamView();

    assertThatThrownBy(() -> view.mark(0)).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(view::reset).isInstanceOf(UnsupportedOperationException.class);
  }

  private static org.apache.flink.core.memory.DataInputView viewOver(byte[] bytes) {
    return new DataInputViewStreamWrapper(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
