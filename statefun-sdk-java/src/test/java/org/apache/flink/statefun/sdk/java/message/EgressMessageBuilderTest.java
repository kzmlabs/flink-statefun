// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.message;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.junit.jupiter.api.Test;

class EgressMessageBuilderTest {

  private static final TypeName EGRESS = TypeName.typeNameOf("io.test", "egress");

  @Test
  void roundtripsLongValueViaCustomType() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue(100L).build();

    assertThat(m.targetEgressId(), is(equalTo(EGRESS)));
    assertThat(m.egressMessageValueType(), is(equalTo(Types.longType().typeName())));
    assertEquals(100L, decode(m, Types.longType()));
  }

  @Test
  void roundtripsIntegerValue() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue(7).build();

    assertEquals(7, (int) decode(m, Types.integerType()));
  }

  @Test
  void roundtripsBooleanValue() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue(true).build();

    assertEquals(true, decode(m, Types.booleanType()));
  }

  @Test
  void roundtripsStringValue() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue("hello").build();

    assertEquals("hello", decode(m, Types.stringType()));
  }

  @Test
  void roundtripsFloatValue() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue(2.5f).build();

    assertEquals(2.5f, decode(m, Types.floatType()));
  }

  @Test
  void roundtripsDoubleValue() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue(2.5d).build();

    assertEquals(2.5d, decode(m, Types.doubleType()));
  }

  @Test
  void byteAccessorReturnsValueBytes() {
    EgressMessage m = EgressMessageBuilder.forEgress(EGRESS).withValue("ascii").build();

    // egressMessageValueBytes is the public accessor downstream consumers call.
    assertThat(m.egressMessageValueBytes(), notNullValue());
    assertThat(m.egressMessageValueBytes().readableBytes() > 0, is(true));
  }

  @Test
  void forEgressRejectsNullTarget() {
    assertThrows(NullPointerException.class, () -> EgressMessageBuilder.forEgress(null));
  }

  @Test
  void withCustomTypeRejectsNullType() {
    EgressMessageBuilder b = EgressMessageBuilder.forEgress(EGRESS);
    assertThrows(NullPointerException.class, () -> b.withCustomType(null, "x"));
  }

  @Test
  void withCustomTypeRejectsNullElement() {
    EgressMessageBuilder b = EgressMessageBuilder.forEgress(EGRESS);
    assertThrows(NullPointerException.class, () -> b.withCustomType(Types.stringType(), null));
  }

  private static <T> T decode(EgressMessage m, org.apache.flink.statefun.sdk.java.types.Type<T> t) {
    return t.typeSerializer().deserialize(m.egressMessageValueBytes());
  }
}
