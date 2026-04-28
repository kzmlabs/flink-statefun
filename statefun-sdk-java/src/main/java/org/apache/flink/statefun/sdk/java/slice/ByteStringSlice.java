// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.java.slice;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;

final class ByteStringSlice implements Slice {
  private final ByteString byteString;

  public ByteStringSlice(ByteString bytes) {
    this.byteString = Objects.requireNonNull(bytes);
  }

  public ByteString byteString() {
    return byteString;
  }

  @Override
  public ByteBuffer asReadOnlyByteBuffer() {
    return byteString.asReadOnlyByteBuffer();
  }

  @Override
  public int readableBytes() {
    return byteString.size();
  }

  @Override
  public void copyTo(byte[] target) {
    copyTo(target, 0);
  }

  @Override
  public void copyTo(byte[] target, int targetOffset) {
    byteString.copyTo(target, targetOffset);
  }

  @Override
  public void copyTo(OutputStream outputStream) {
    try {
      byteString.writeTo(outputStream);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public byte byteAt(int position) {
    return byteString.byteAt(position);
  }

  @Override
  public void copyTo(ByteBuffer buffer) {
    byteString.copyTo(buffer);
  }

  @Override
  public byte[] toByteArray() {
    return byteString.toByteArray();
  }
}
