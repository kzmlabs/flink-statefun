// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.message;

import static org.apache.flink.statefun.flink.core.TestUtils.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.stream.Stream;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class MessageTest {

  static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(MessageFactoryType.WITH_KRYO_PAYLOADS, null, DUMMY_PAYLOAD),
        Arguments.of(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null, DUMMY_PAYLOAD),
        Arguments.of(MessageFactoryType.WITH_RAW_PAYLOADS, null, DUMMY_PAYLOAD.toByteArray()),
        Arguments.of(
            MessageFactoryType.WITH_CUSTOM_PAYLOADS,
            "org.apache.flink.statefun.flink.core.message.JavaPayloadSerializer",
            DUMMY_PAYLOAD));
  }

  @ParameterizedTest
  @MethodSource("data")
  void roundTrip(MessageFactoryType type, String customPayloadSerializerClassName, Object payload)
      throws IOException {
    MessageFactory factory =
        MessageFactory.forKey(MessageFactoryKey.forType(type, customPayloadSerializerClassName));

    Message fromSdk = factory.from(FUNCTION_1_ADDR, FUNCTION_2_ADDR, payload);
    DataOutputSerializer out = new DataOutputSerializer(32);
    fromSdk.writeTo(factory, out);

    Message fromEnvelope = factory.from(new DataInputDeserializer(out.getCopyOfBuffer()));

    assertThat(fromEnvelope.source(), is(FUNCTION_1_ADDR));
    assertThat(fromEnvelope.target(), is(FUNCTION_2_ADDR));

    ClassLoader targetClassLoader = payload.getClass().getClassLoader();
    Object actualPayload = fromEnvelope.payload(factory, targetClassLoader);

    assertThat(actualPayload, is(payload));
  }
}
