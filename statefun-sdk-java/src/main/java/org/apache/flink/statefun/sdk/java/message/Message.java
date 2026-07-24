// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.sdk.java.message;

import java.util.Collections;
import java.util.List;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.types.Type;

public interface Message {
  Address targetAddress();

  /**
   * Transport-level headers attached to this message. For messages delivered from a Kafka
   * ingress, these are the headers of the consumed Kafka record. Messages without headers (for
   * example function-to-function messages) return an empty list.
   */
  default List<MessageHeader> headers() {
    return Collections.emptyList();
  }

  boolean isLong();

  long asLong();

  boolean isUtf8String();

  String asUtf8String();

  boolean isInt();

  int asInt();

  boolean isBoolean();

  boolean asBoolean();

  boolean isFloat();

  float asFloat();

  boolean isDouble();

  double asDouble();

  <T> boolean is(Type<T> type);

  <T> T as(Type<T> type);

  TypeName valueTypeName();

  Slice rawValue();
}
