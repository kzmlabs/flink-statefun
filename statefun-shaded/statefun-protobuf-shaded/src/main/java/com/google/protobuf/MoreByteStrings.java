// SPDX-License-Identifier: Apache-2.0
package com.google.protobuf;

import java.nio.ByteBuffer;

public class MoreByteStrings {

  public static ByteString wrap(byte[] bytes) {
    return ByteString.wrap(bytes);
  }

  public static ByteString wrap(byte[] bytes, int offset, int len) {
    return ByteString.wrap(bytes, offset, len);
  }

  public static ByteString wrap(ByteBuffer buffer) {
    return ByteString.wrap(buffer);
  }

  public static ByteString concat(ByteString first, ByteString second) {
    return first.concat(second);
  }
}
