// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.message;

import com.google.protobuf.ByteString;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.statefun.flink.core.generated.Payload;

@NotThreadSafe
public class MessagePayloadSerializerRaw implements MessagePayloadSerializer {

  @Override
  public Object deserialize(@Nonnull ClassLoader targetClassLoader, @Nonnull Payload payload) {
    return payload.getPayloadBytes().toByteArray();
  }

  @Override
  public Payload serialize(@Nonnull Object what) {
    byte[] bytes = (byte[]) what;
    ByteString bs = ByteString.copyFrom(bytes);
    return Payload.newBuilder().setPayloadBytes(bs).build();
  }

  @Override
  public Object copy(@Nonnull ClassLoader targetClassLoader, @Nonnull Object what) {
    return what;
  }
}
