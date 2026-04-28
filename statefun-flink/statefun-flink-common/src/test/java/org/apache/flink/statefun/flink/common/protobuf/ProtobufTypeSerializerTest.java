// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.common.protobuf;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.statefun.flink.common.protobuf.generated.TestProtos;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ProtobufTypeSerializerTest extends SerializerTestBase<TestProtos.SimpleMessage> {

  @Override
  protected TypeSerializer<TestProtos.SimpleMessage> createSerializer() {
    return new ProtobufTypeSerializer<>(TestProtos.SimpleMessage.class);
  }

  @Disabled
  @Test()
  @Override
  public void testInstantiate() {
    // do nothing.
  }

  @Override
  protected int getLength() {
    return -1;
  }

  @Override
  protected Class<TestProtos.SimpleMessage> getTypeClass() {
    return TestProtos.SimpleMessage.class;
  }

  @Override
  protected TestProtos.SimpleMessage[] getTestData() {
    return new TestProtos.SimpleMessage[] {
      TestProtos.SimpleMessage.newBuilder().setName("a").build(),
      TestProtos.SimpleMessage.newBuilder().setName("b").build(),
      TestProtos.SimpleMessage.newBuilder().setName("c").build(),
      TestProtos.SimpleMessage.newBuilder().setName("d").build()
    };
  }
}
