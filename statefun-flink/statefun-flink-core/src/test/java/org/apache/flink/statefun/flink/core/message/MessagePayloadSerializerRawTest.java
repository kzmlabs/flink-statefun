// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.message;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.statefun.flink.core.generated.Payload;
import org.junit.jupiter.api.Test;

class MessagePayloadSerializerRawTest {

  private final MessagePayloadSerializerRaw serializer = new MessagePayloadSerializerRaw();

  @Test
  void roundtripPreservesByteContent() {
    byte[] original = new byte[] {1, 2, 3, (byte) 0xFF, 0, 7};

    Payload payload = serializer.serialize(original);
    byte[] roundtripped =
        (byte[]) serializer.deserialize(getClass().getClassLoader(), payload);

    assertThat(roundtripped).containsExactly(original).isNotSameAs(original);
  }

  @Test
  void roundtripWithEmptyPayload() {
    Payload payload = serializer.serialize(new byte[0]);

    byte[] result = (byte[]) serializer.deserialize(getClass().getClassLoader(), payload);

    assertThat(result).isEmpty();
  }

  @Test
  void copyReturnsSameInstanceBecauseRawByteArraysAreImmutableInUseHere() {
    // The Raw serializer is a no-op copy — payloads are byte arrays the runtime treats
    // as opaque bytes. The contract is "same reference" — pin it so a future "defensive
    // copy" change doesn't go undetected (it would change observed identity in tight loops).
    byte[] original = new byte[] {10, 20};

    Object copy = serializer.copy(getClass().getClassLoader(), original);

    assertThat(copy).isSameAs(original);
  }
}
