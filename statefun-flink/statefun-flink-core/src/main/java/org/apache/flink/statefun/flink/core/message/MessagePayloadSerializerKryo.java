// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.message;

import com.google.protobuf.ByteString;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.statefun.flink.core.generated.Payload;

public final class MessagePayloadSerializerKryo implements MessagePayloadSerializer {

  private KryoSerializer<Object> kryo =
      new KryoSerializer<>(Object.class, new ExecutionConfig().getSerializerConfig());
  private DataInputDeserializer source = new DataInputDeserializer();
  private DataOutputSerializer target = new DataOutputSerializer(4096);

  @Override
  public Payload serialize(@Nonnull Object payloadObject) {
    target.clear();
    try {
      kryo.serialize(payloadObject, target);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    ByteString serializedBytes = ByteString.copyFrom(target.getSharedBuffer(), 0, target.length());
    return Payload.newBuilder()
        .setClassName(payloadObject.getClass().getName())
        .setPayloadBytes(serializedBytes)
        .build();
  }

  @Override
  public Object deserialize(@Nonnull ClassLoader targetClassLoader, @Nonnull Payload payload) {
    source.setBuffer(payload.getPayloadBytes().asReadOnlyByteBuffer());
    try {
      return kryo.deserialize(source);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Object copy(@Nonnull ClassLoader targetClassLoader, @Nonnull Object what) {
    target.clear();
    try {
      kryo.serialize(what, target);
      source.setBuffer(target.getSharedBuffer(), 0, target.length());

      final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(targetClassLoader);
      try {
        final ClassLoader originalKryoCl = kryo.getKryo().getClassLoader();
        kryo.getKryo().setClassLoader(targetClassLoader);
        try {
          return kryo.deserialize(source);
        } finally {
          kryo.getKryo().setClassLoader(originalKryoCl);
        }
      } finally {
        Thread.currentThread().setContextClassLoader(currentClassLoader);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
