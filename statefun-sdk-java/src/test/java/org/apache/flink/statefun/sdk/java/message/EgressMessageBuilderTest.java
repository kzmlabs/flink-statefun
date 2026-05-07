// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.junit.jupiter.api.Test;

class EgressMessageBuilderTest {

  private static final TypeName EGRESS = TypeName.typeNameOf("io.test", "egress");

  @Test
  void roundtripsLongValueViaCustomType() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue(100L).build();

    assertThat(m.targetEgressId()).isEqualTo(EGRESS);
    assertThat(m.egressMessageValueType()).isEqualTo(Types.longType().typeName());
    assertThat(decode(m, Types.longType())).isEqualTo(100L);
  }

  @Test
  void roundtripsIntegerValue() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue(7).build();

    assertThat(decode(m, Types.integerType())).isEqualTo(7);
  }

  @Test
  void roundtripsBooleanValue() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue(true).build();

    assertThat(decode(m, Types.booleanType())).isTrue();
  }

  @Test
  void roundtripsStringValue() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue("hello").build();

    assertThat(decode(m, Types.stringType())).isEqualTo("hello");
  }

  @Test
  void roundtripsFloatValue() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue(2.5f).build();

    assertThat(decode(m, Types.floatType())).isEqualTo(2.5f);
  }

  @Test
  void roundtripsDoubleValue() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue(2.5d).build();

    assertThat(decode(m, Types.doubleType())).isEqualTo(2.5d);
  }

  @Test
  void byteAccessorReturnsValueBytes() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue("ascii").build();

    // egressMessageValueBytes is the public accessor downstream consumers call.
    assertThat(m.egressMessageValueBytes()).isNotNull();
    assertThat(m.egressMessageValueBytes().readableBytes()).isPositive();
  }

  @Test
  void forEgressRejectsNullTarget() {
    assertThatThrownBy(() -> EgressMessageBuilder.forEgress(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void withCustomTypeRejectsNullType() {
    EgressMessageBuilder b = EgressMessageBuilder.forEgress(EGRESS);
    assertThatThrownBy(() -> b.withCustomType(null, "x"))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void withCustomTypeRejectsNullElement() {
    EgressMessageBuilder b = EgressMessageBuilder.forEgress(EGRESS);
    assertThatThrownBy(() -> b.withCustomType(Types.stringType(), null))
        .isInstanceOf(NullPointerException.class);
  }

  private static <T> T decode(EgressMessage m, Type<T> t) {
    return t.typeSerializer().deserialize(m.egressMessageValueBytes());
  }
}
