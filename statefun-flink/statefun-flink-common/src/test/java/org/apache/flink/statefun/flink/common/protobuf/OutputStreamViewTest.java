// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.common.protobuf;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.jupiter.api.Test;

class OutputStreamViewTest {

  @Test
  void singleByteWriteIsForwardedToUnderlyingDataOutput() throws Exception {
    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    OutputStreamView view = new OutputStreamView();
    view.set(new DataOutputViewStreamWrapper(new DataOutputStream(sink)));

    view.write(0x42);

    assertThat(sink.toByteArray()).containsExactly(0x42);
  }

  @Test
  void byteArrayWriteIsForwarded() throws Exception {
    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    OutputStreamView view = new OutputStreamView();
    view.set(new DataOutputViewStreamWrapper(new DataOutputStream(sink)));

    view.write(new byte[] {1, 2, 3, 4});

    assertThat(sink.toByteArray()).containsExactly(1, 2, 3, 4);
  }

  @Test
  void byteArrayOffsetLenWriteIsForwarded() throws Exception {
    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    OutputStreamView view = new OutputStreamView();
    view.set(new DataOutputViewStreamWrapper(new DataOutputStream(sink)));

    view.write(new byte[] {1, 2, 3, 4, 5}, 1, 3);

    // Mid-buffer slice — bytes 2,3,4.
    assertThat(sink.toByteArray()).containsExactly(2, 3, 4);
  }

  @Test
  void flushAndCloseAreNoOps() throws Exception {
    OutputStreamView view = new OutputStreamView();
    // No target set — flush/close must not NPE; the stream is allowed to be reused.
    view.flush();
    view.close();
    // No exception means pass.
    assertThat(view).isNotNull();
  }

  @Test
  void doneDetachesTarget() {
    OutputStreamView view = new OutputStreamView();
    view.set(new DataOutputViewStreamWrapper(new DataOutputStream(new ByteArrayOutputStream())));

    view.done();

    // After done, no target — write would NPE. The contract is that done() releases the
    // underlying view so the OutputStreamView can be cached/pooled without holding a stale
    // reference to last-job's serialization buffer.
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> view.write(0))
        .isInstanceOf(NullPointerException.class);
  }
}
