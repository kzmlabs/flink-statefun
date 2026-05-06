// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.message;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.jupiter.api.Test;

class MessageBuilderTest {

  private static final TypeName FN = TypeName.typeNameOf("counter", "increment");

  @Test
  void roundtripsLongValue() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue(42L).build();

    assertTrue(m.isLong());
    assertEquals(42L, m.asLong());
    assertFalse(m.isInt());
    assertFalse(m.isUtf8String());
  }

  @Test
  void roundtripsIntValue() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue(7).build();

    assertTrue(m.isInt());
    assertEquals(7, m.asInt());
  }

  @Test
  void roundtripsBooleanValue() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue(true).build();

    assertTrue(m.isBoolean());
    assertTrue(m.asBoolean());
  }

  @Test
  void roundtripsStringValue() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue("hello").build();

    assertTrue(m.isUtf8String());
    assertEquals("hello", m.asUtf8String());
  }

  @Test
  void roundtripsFloatValue() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue(3.14f).build();

    assertTrue(m.isFloat());
    assertEquals(3.14f, m.asFloat());
  }

  @Test
  void roundtripsDoubleValue() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue(3.14159265).build();

    assertTrue(m.isDouble());
    assertEquals(3.14159265, m.asDouble());
  }

  @Test
  void typeMismatchOnAccessReflectsEncodedType() {
    // is*() reflects the encoded type tag — pin it so a future "tolerant decoder" doesn't
    // silently accept incorrect payloads.
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue("not-a-long").build();

    assertFalse(m.isLong());
    assertTrue(m.isUtf8String());
  }

  @Test
  void valueTypeNameReportsEncodedType() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue(1L).build();

    assertThat(m.valueTypeName(), is(equalTo(Types.longType().typeName())));
  }

  @Test
  void rawValueReadsBackByteContent() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue("ascii").build();

    assertThat(m.rawValue(), notNullValue());
    assertThat(m.rawValue().readableBytes(), not(is(0)));
  }

  @Test
  void targetAddressFromTypeNameAndIdMatchesAddressFactory() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue(1L).build();

    assertThat(m.targetAddress(), is(equalTo(new Address(FN, "id-1"))));
  }

  @Test
  void forAddressOverloadAcceptsAddressInstance() {
    Address addr = new Address(FN, "x");
    Message m = MessageBuilder.forAddress(addr).withValue(1L).build();

    assertThat(m.targetAddress(), is(equalTo(addr)));
  }

  @Test
  void forAddressRejectsNullAddress() {
    assertThrows(NullPointerException.class, () -> MessageBuilder.forAddress((Address) null));
  }

  @Test
  void withTargetAddressOverridesPreviouslySetAddress() {
    Address newTarget = new Address(TypeName.typeNameOf("other", "fn"), "id-2");

    Message m =
        MessageBuilder.forAddress(FN, "id-1")
            .withValue(1L)
            .withTargetAddress(newTarget)
            .build();

    assertThat(m.targetAddress(), is(equalTo(newTarget)));
  }

  @Test
  void withTargetAddressTypeNameAndIdOverloadDelegates() {
    TypeName otherType = TypeName.typeNameOf("other", "fn");

    Message m =
        MessageBuilder.forAddress(FN, "id-1")
            .withValue(1L)
            .withTargetAddress(otherType, "id-2")
            .build();

    assertThat(m.targetAddress(), is(equalTo(new Address(otherType, "id-2"))));
  }

  @Test
  void withCustomTypeRejectsNullType() {
    MessageBuilder builder = MessageBuilder.forAddress(FN, "id-1");
    assertThrows(NullPointerException.class, () -> builder.withCustomType(null, "x"));
  }

  @Test
  void withCustomTypeRejectsNullElement() {
    MessageBuilder builder = MessageBuilder.forAddress(FN, "id-1");
    assertThrows(
        NullPointerException.class, () -> builder.withCustomType(Types.stringType(), null));
  }

  @Test
  void fromMessageProducesEqualMessage() {
    Message original = MessageBuilder.forAddress(FN, "id-1").withValue("payload").build();

    Message rebuilt = MessageBuilder.fromMessage(original).build();

    assertThat(rebuilt, is(equalTo(original)));
  }

  @Test
  void fromMessageAllowsTargetAddressOverride() {
    Message original = MessageBuilder.forAddress(FN, "id-1").withValue("payload").build();
    Address newTarget = new Address(TypeName.typeNameOf("other", "fn"), "id-2");

    Message rebuilt =
        MessageBuilder.fromMessage(original).withTargetAddress(newTarget).build();

    assertThat(rebuilt.targetAddress(), is(equalTo(newTarget)));
    assertThat(rebuilt.asUtf8String(), is(equalTo("payload")));
  }

  @Test
  void messageWrapperRejectsTypedValueWithoutHasValue() {
    Address addr = new Address(FN, "id-1");
    TypedValue empty = TypedValue.newBuilder().build();

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> new MessageWrapper(addr, empty));
    assertThat(ex.getMessage(), containsString("Unset empty Messages are prohibited"));
  }

  @Test
  void equalsAndHashCodeRespectAddressAndPayload() {
    Message a = MessageBuilder.forAddress(FN, "id-1").withValue("v").build();
    Message b = MessageBuilder.forAddress(FN, "id-1").withValue("v").build();
    Message differentAddress = MessageBuilder.forAddress(FN, "id-2").withValue("v").build();
    Message differentValue = MessageBuilder.forAddress(FN, "id-1").withValue("w").build();

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertThat(a, is(not(equalTo(differentAddress))));
    assertThat(a, is(not(equalTo(differentValue))));
  }

  @Test
  void equalsToSelfAndNotToNullOrUnrelated() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue("v").build();
    assertEquals(m, m);
    assertThat(m, is(not(equalTo(null))));
    assertThat((Object) m, is(not(equalTo((Object) "string"))));
  }

  @Test
  void toStringIncludesTargetAddress() {
    // Implementation has a toString that's used in error logs — pin presence of address.
    Message m = MessageBuilder.forAddress(FN, "id-xyz").withValue(1L).build();
    assertThat(m.toString(), containsString("id-xyz"));
  }
}
