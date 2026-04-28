// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.java.slice;

import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.MoreByteStrings;

public final class Slices {
  private Slices() {}

  public static Slice wrap(ByteBuffer buffer) {
    return wrap(MoreByteStrings.wrap(buffer));
  }

  public static Slice wrap(byte[] bytes) {
    return wrap(MoreByteStrings.wrap(bytes));
  }

  private static Slice wrap(ByteString bytes) {
    return new ByteStringSlice(bytes);
  }

  public static Slice wrap(byte[] bytes, int offset, int len) {
    return wrap(MoreByteStrings.wrap(bytes, offset, len));
  }

  public static Slice copyOf(byte[] bytes) {
    return wrap(ByteString.copyFrom(bytes));
  }

  public static Slice copyOf(byte[] bytes, int offset, int len) {
    return wrap(ByteString.copyFrom(bytes, offset, len));
  }

  public static Slice copyOf(InputStream inputStream, int expectedStreamSize) {
    SliceOutput out = SliceOutput.sliceOutput(expectedStreamSize);
    out.writeFully(inputStream);
    return out.view();
  }

  public static Slice copyFromUtf8(String input) {
    return wrap(ByteString.copyFromUtf8(input));
  }
}
