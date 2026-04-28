// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.message;

import com.google.protobuf.ByteString;
import java.io.*;
import javax.annotation.Nonnull;
import org.apache.flink.statefun.flink.core.generated.Payload;

// this is a payload serializer that uses normal java serialization, used for testing custom payload
// serialization
public class JavaPayloadSerializer implements MessagePayloadSerializer {

  @Override
  public Payload serialize(@Nonnull Object payloadObject) {
    try {
      String className = payloadObject.getClass().getName();
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
          out.writeObject(payloadObject);
          out.flush();
          byte[] bytes = bos.toByteArray();
          return Payload.newBuilder()
              .setClassName(className)
              .setPayloadBytes(ByteString.copyFrom(bytes))
              .build();
        }
      }
    } catch (Throwable ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Object deserialize(@Nonnull ClassLoader targetClassLoader, @Nonnull Payload payload) {
    try {
      try (ByteArrayInputStream bis =
          new ByteArrayInputStream(payload.getPayloadBytes().toByteArray())) {
        try (ObjectInput in = new ObjectInputStream(bis)) {
          return in.readObject();
        }
      }
    } catch (Throwable ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Object copy(@Nonnull ClassLoader targetClassLoader, @Nonnull Object what) {
    return deserialize(targetClassLoader, serialize(what));
  }
}
