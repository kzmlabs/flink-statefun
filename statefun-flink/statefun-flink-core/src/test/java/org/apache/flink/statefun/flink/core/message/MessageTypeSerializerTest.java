// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.message;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.LongStream;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.statefun.flink.core.TestUtils;
import org.apache.flink.testutils.DeeplyEqualsChecker;
import org.junit.jupiter.api.Disabled;

public class MessageTypeSerializerTest extends SerializerTestBase<Message> {

  public MessageTypeSerializerTest() {
    super(
        new DeeplyEqualsChecker() {
          @Override
          public boolean deepEquals(Object o1, Object o2) {
            Message a = (Message) o1;
            Message b = (Message) o2;
            DataOutputSerializer aOut = new DataOutputSerializer(32);
            DataOutputSerializer bOut = new DataOutputSerializer(32);
            MessageFactory factory =
                MessageFactory.forKey(
                    MessageFactoryKey.forType(MessageFactoryType.WITH_KRYO_PAYLOADS, null));
            try {
              a.writeTo(factory, aOut);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            try {
              b.writeTo(factory, bOut);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            return Arrays.equals(aOut.getCopyOfBuffer(), bOut.getCopyOfBuffer());
          }
        });
  }

  @Override
  protected TypeSerializer<Message> createSerializer() {
    return new MessageTypeInformation(
            MessageFactoryKey.forType(MessageFactoryType.WITH_KRYO_PAYLOADS, null))
        .createSerializer(new ExecutionConfig().getSerializerConfig());
  }

  @Override
  protected int getLength() {
    return -1;
  }

  @Override
  protected Class<Message> getTypeClass() {
    return Message.class;
  }

  @Override
  protected Message[] getTestData() {
    return LongStream.range(1, 100)
        .mapToObj(TestUtils.ENVELOPE_FACTORY::from)
        .toArray(Message[]::new);
  }

  @Disabled
  @Override
  public void testInstantiate() {}
}
