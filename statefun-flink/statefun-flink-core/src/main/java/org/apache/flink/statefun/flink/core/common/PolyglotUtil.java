// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.common;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.io.IOException;
import java.io.InputStream;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.Address;

public final class PolyglotUtil {
  private PolyglotUtil() {}

  public static <M extends Message> M parseProtobufOrThrow(Parser<M> parser, InputStream input) {
    try {
      return parser.parseFrom(input);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to parse a Protobuf message", e);
    }
  }

  public static Address sdkAddressToPolyglotAddress(
      org.apache.flink.statefun.sdk.Address sdkAddress) {
    return Address.newBuilder()
        .setNamespace(sdkAddress.type().namespace())
        .setType(sdkAddress.type().name())
        .setId(sdkAddress.id())
        .build();
  }

  public static org.apache.flink.statefun.sdk.Address polyglotAddressToSdkAddress(Address address) {
    return new org.apache.flink.statefun.sdk.Address(
        new FunctionType(address.getNamespace(), address.getType()), address.getId());
  }
}
