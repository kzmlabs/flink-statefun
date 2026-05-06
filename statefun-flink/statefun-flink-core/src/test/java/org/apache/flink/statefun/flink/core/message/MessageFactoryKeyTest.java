// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class MessageFactoryKeyTest {

  @Test
  void typeAndCustomSerializerArePreserved() {
    MessageFactoryKey key =
        MessageFactoryKey.forType(MessageFactoryType.WITH_KRYO_PAYLOADS, "com.foo.Bar");

    assertThat(key.getType()).isEqualTo(MessageFactoryType.WITH_KRYO_PAYLOADS);
    assertThat(key.getCustomPayloadSerializerClassName()).hasValue("com.foo.Bar");
  }

  @Test
  void nullCustomSerializerProducesEmptyOptional() {
    MessageFactoryKey key = MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null);

    assertThat(key.getCustomPayloadSerializerClassName()).isEmpty();
  }

  @Test
  void equalsAndHashCodeRespectBothFields() {
    MessageFactoryKey a = MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, "x");
    MessageFactoryKey b = MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, "x");
    MessageFactoryKey differentType =
        MessageFactoryKey.forType(MessageFactoryType.WITH_KRYO_PAYLOADS, "x");
    MessageFactoryKey differentCustom =
        MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, "y");

    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
    assertThat(a).isNotEqualTo(differentType).isNotEqualTo(differentCustom);
  }

  @Test
  void equalsHandlesNullCustomSerializerSymmetrically() {
    MessageFactoryKey withNull =
        MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null);
    MessageFactoryKey alsoWithNull =
        MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null);
    MessageFactoryKey withValue =
        MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, "x");

    assertThat(withNull).isEqualTo(alsoWithNull).isNotEqualTo(withValue);
  }

  @Test
  void equalsToSelfAndNotToNullOrUnrelated() {
    MessageFactoryKey a = MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, "x");
    assertThat(a).isEqualTo(a).isNotEqualTo(null).isNotEqualTo("foo");
  }

  @Test
  void rejectsNullType() {
    assertThatThrownBy(() -> MessageFactoryKey.forType(null, "x"))
        .isInstanceOf(NullPointerException.class);
  }
}
