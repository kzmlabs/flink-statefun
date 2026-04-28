// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import org.apache.flink.statefun.flink.core.generated.EnvelopeAddress;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;

@SuppressWarnings("WeakerAccess")
public class TestUtils {

  public static final MessageFactory ENVELOPE_FACTORY =
      MessageFactory.forKey(MessageFactoryKey.forType(MessageFactoryType.WITH_KRYO_PAYLOADS, null));

  public static final FunctionType FUNCTION_TYPE = new FunctionType("test", "a");
  public static final Address FUNCTION_1_ADDR = new Address(FUNCTION_TYPE, "a-1");
  public static final Address FUNCTION_2_ADDR = new Address(FUNCTION_TYPE, "a-2");
  public static final EnvelopeAddress DUMMY_PAYLOAD =
      EnvelopeAddress.newBuilder().setNamespace("com.foo").setType("greet").setId("user-1").build();

  /**
   * Opens a stream of throws an exception. Does *not* close the stream
   *
   * @param url of the resource to open
   * @return opened input stream
   */
  public static InputStream openStreamOrThrow(URL url) {
    try {
      return url.openStream();
    } catch (IOException e) {
      throw new IllegalStateException("Could not open " + url.getPath(), e);
    }
  }
}
