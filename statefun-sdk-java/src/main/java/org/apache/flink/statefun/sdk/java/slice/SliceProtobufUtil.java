// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.java.slice;

import org.apache.flink.statefun.sdk.java.annotations.Internal;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.Message;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.MoreByteStrings;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.Parser;

@Internal
public final class SliceProtobufUtil {
  private SliceProtobufUtil() {}

  public static <T> T parseFrom(Parser<T> parser, Slice slice)
      throws InvalidProtocolBufferException {
    if (slice instanceof ByteStringSlice) {
      ByteString byteString = ((ByteStringSlice) slice).byteString();
      return parser.parseFrom(byteString);
    }
    return parser.parseFrom(slice.asReadOnlyByteBuffer());
  }

  public static Slice toSlice(Message message) {
    return Slices.wrap(message.toByteArray());
  }

  public static ByteString asByteString(Slice slice) {
    if (slice instanceof ByteStringSlice) {
      ByteStringSlice byteStringSlice = (ByteStringSlice) slice;
      return byteStringSlice.byteString();
    }
    return MoreByteStrings.wrap(slice.asReadOnlyByteBuffer());
  }

  public static Slice asSlice(ByteString byteString) {
    return new ByteStringSlice(byteString);
  }
}
