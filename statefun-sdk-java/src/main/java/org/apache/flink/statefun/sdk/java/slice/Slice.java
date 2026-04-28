// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.java.slice;

import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface Slice {

  int readableBytes();

  void copyTo(ByteBuffer target);

  void copyTo(byte[] target);

  void copyTo(byte[] target, int targetOffset);

  void copyTo(OutputStream outputStream);

  byte byteAt(int position);

  ByteBuffer asReadOnlyByteBuffer();

  byte[] toByteArray();
}
