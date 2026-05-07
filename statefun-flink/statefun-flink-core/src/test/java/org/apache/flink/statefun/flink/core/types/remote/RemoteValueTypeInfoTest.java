// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.types.remote;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.statefun.sdk.TypeName;
import org.junit.jupiter.api.Test;

class RemoteValueTypeInfoTest {

  private static final TypeName TYPE = new TypeName("io.test", "remote");
  private static final TypeName OTHER = new TypeName("io.test", "other");

  @Test
  void exposesByteArrayShapeAsScalar() {
    RemoteValueTypeInfo info = new RemoteValueTypeInfo(TYPE);

    // RemoteValueTypeInfo wraps a byte[] payload and is treated as a scalar by Flink:
    // not a basic type, not a tuple, arity 0, total fields 0, not a key.
    assertThat(info.isBasicType()).isFalse();
    assertThat(info.isTupleType()).isFalse();
    assertThat(info.getArity()).isZero();
    assertThat(info.getTotalFields()).isZero();
    assertThat(info.isKeyType()).isFalse();
    assertThat(info.getTypeClass()).isEqualTo(byte[].class);
  }

  @Test
  void createSerializerReturnsRemoteValueSerializerForType() {
    RemoteValueTypeInfo info = new RemoteValueTypeInfo(TYPE);

    TypeSerializer<byte[]> serializer = info.createSerializer(new SerializerConfigImpl());

    assertThat(serializer).isInstanceOf(RemoteValueSerializer.class);
    assertThat(((RemoteValueSerializer) serializer).getType()).isEqualTo(TYPE);
  }

  @Test
  void rejectsNullTypeName() {
    assertThatThrownBy(() -> new RemoteValueTypeInfo(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void equalsAndHashCodeIdentifyByTypeName() {
    RemoteValueTypeInfo a = new RemoteValueTypeInfo(TYPE);
    RemoteValueTypeInfo aAgain = new RemoteValueTypeInfo(new TypeName("io.test", "remote"));
    RemoteValueTypeInfo b = new RemoteValueTypeInfo(OTHER);

    assertThat(a).isEqualTo(a).isEqualTo(aAgain).isNotEqualTo(b);
    assertThat(a).isNotEqualTo(null).isNotEqualTo("not a typeinfo");
    assertThat(a.hashCode()).isEqualTo(aAgain.hashCode());
  }

  @Test
  void canEqualOnlyOtherRemoteValueTypeInfo() {
    RemoteValueTypeInfo info = new RemoteValueTypeInfo(TYPE);

    assertThat(info.canEqual(new RemoteValueTypeInfo(OTHER))).isTrue();
    assertThat(info.canEqual("not a typeinfo")).isFalse();
  }

  @Test
  void toStringIncludesType() {
    RemoteValueTypeInfo info = new RemoteValueTypeInfo(TYPE);

    assertThat(info.toString()).contains("RemoteValueTypeInfo").contains(TYPE.toString());
  }
}
