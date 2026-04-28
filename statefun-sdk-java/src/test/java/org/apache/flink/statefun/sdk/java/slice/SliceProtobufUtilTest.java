// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.java.slice;

import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

public class SliceProtobufUtilTest {

  @Test
  public void usageExample() {
    ByteString expected = ByteString.copyFromUtf8("Hello world");

    Slice slice = SliceProtobufUtil.asSlice(expected);
    ByteString got = SliceProtobufUtil.asByteString(slice);

    assertSame(expected, got, "Expecting the same reference.");
  }
}
