// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.common.types;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class TypedValueUtil {

  private TypedValueUtil() {}

  public static boolean isProtobufTypeOf(
      TypedValue typedValue, Descriptors.Descriptor messageDescriptor) {
    return typedValue.getTypename().equals(protobufTypeUrl(messageDescriptor));
  }

  public static TypedValue packProtobufMessage(Message protobufMessage) {
    return TypedValue.newBuilder()
        .setTypename(protobufTypeUrl(protobufMessage.getDescriptorForType()))
        .setHasValue(true)
        .setValue(protobufMessage.toByteString())
        .build();
  }

  public static <PB extends Message> PB unpackProtobufMessage(
      TypedValue typedValue, Parser<PB> protobufMessageParser) {
    try {
      return protobufMessageParser.parseFrom(typedValue.getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String protobufTypeUrl(Descriptors.Descriptor messageDescriptor) {
    return "type.googleapis.com/" + messageDescriptor.getFullName();
  }
}
