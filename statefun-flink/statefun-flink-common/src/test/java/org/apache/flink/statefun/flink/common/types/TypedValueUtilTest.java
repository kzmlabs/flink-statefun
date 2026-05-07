// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.common.types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.jupiter.api.Test;

class TypedValueUtilTest {

  @Test
  void packProtobufMessageProducesTypeUrlPrefixedByTypeGoogleapisCom() {
    TypedValue input = TypedValue.newBuilder().setTypename("foo/bar").setHasValue(true).build();

    TypedValue packed = TypedValueUtil.packProtobufMessage(input);

    assertThat(packed.getTypename())
        .isEqualTo("type.googleapis.com/" + TypedValue.getDescriptor().getFullName());
    assertThat(packed.getHasValue()).isTrue();
    assertThat(packed.getValue()).isEqualTo(input.toByteString());
  }

  @Test
  void isProtobufTypeOfMatchesPackedDescriptor() {
    TypedValue input = TypedValue.newBuilder().setTypename("anything").build();
    TypedValue packed = TypedValueUtil.packProtobufMessage(input);

    assertThat(TypedValueUtil.isProtobufTypeOf(packed, TypedValue.getDescriptor())).isTrue();
  }

  @Test
  void isProtobufTypeOfRejectsMismatchedTypeName() {
    TypedValue notPacked =
        TypedValue.newBuilder().setTypename("type.googleapis.com/some.Other").build();

    assertThat(TypedValueUtil.isProtobufTypeOf(notPacked, TypedValue.getDescriptor())).isFalse();
  }

  @Test
  void unpackProtobufMessageRoundtripsValue() {
    TypedValue original = TypedValue.newBuilder().setTypename("io.test/x").setHasValue(true).build();
    TypedValue packed = TypedValueUtil.packProtobufMessage(original);

    TypedValue unpacked = TypedValueUtil.unpackProtobufMessage(packed, TypedValue.parser());

    assertThat(unpacked).isEqualTo(original);
  }

  @Test
  void unpackProtobufMessageRaisesIllegalStateOnCorruptedBytes() {
    TypedValue corrupted =
        TypedValue.newBuilder()
            .setTypename("type.googleapis.com/" + TypedValue.getDescriptor().getFullName())
            .setHasValue(true)
            .setValue(ByteString.copyFrom(new byte[] {(byte) 0xFF, (byte) 0xFF}))
            .build();

    assertThatThrownBy(() -> TypedValueUtil.unpackProtobufMessage(corrupted, TypedValue.parser()))
        .isInstanceOf(IllegalStateException.class)
        .hasCauseInstanceOf(com.google.protobuf.InvalidProtocolBufferException.class);
  }
}
