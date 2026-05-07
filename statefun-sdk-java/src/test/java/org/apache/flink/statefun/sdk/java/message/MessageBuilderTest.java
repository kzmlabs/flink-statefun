// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    assertThat(m.isLong()).isTrue();
    assertThat(m.asLong()).isEqualTo(42L);
    assertThat(m.isInt()).isFalse();
    assertThat(m.isUtf8String()).isFalse();
  }

  @Test
  void roundtripsIntValue() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue(7).build();

    assertThat(m.isInt()).isTrue();
    assertThat(m.asInt()).isEqualTo(7);
  }

  @Test
  void roundtripsBooleanValue() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue(true).build();

    assertThat(m.isBoolean()).isTrue();
    assertThat(m.asBoolean()).isTrue();
  }

  @Test
  void roundtripsStringValue() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue("hello").build();

    assertThat(m.isUtf8String()).isTrue();
    assertThat(m.asUtf8String()).isEqualTo("hello");
  }

  @Test
  void roundtripsFloatValue() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue(3.14f).build();

    assertThat(m.isFloat()).isTrue();
    assertThat(m.asFloat()).isEqualTo(3.14f);
  }

  @Test
  void roundtripsDoubleValue() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue(3.14159265).build();

    assertThat(m.isDouble()).isTrue();
    assertThat(m.asDouble()).isEqualTo(3.14159265);
  }

  @Test
  void typeMismatchOnAccessReflectsEncodedType() {
    // is*() reflects the encoded type tag — pin it so a future "tolerant decoder" doesn't
    // silently accept incorrect payloads.
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue("not-a-long").build();

    assertThat(m.isLong()).isFalse();
    assertThat(m.isUtf8String()).isTrue();
  }

  @Test
  void valueTypeNameReportsEncodedType() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue(1L).build();

    assertThat(m.valueTypeName()).isEqualTo(Types.longType().typeName());
  }

  @Test
  void rawValueReadsBackByteContent() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue("ascii").build();

    assertThat(m.rawValue()).isNotNull();
    assertThat(m.rawValue().readableBytes()).isPositive();
  }

  @Test
  void targetAddressFromTypeNameAndIdMatchesAddressFactory() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue(1L).build();

    assertThat(m.targetAddress()).isEqualTo(new Address(FN, "id-1"));
  }

  @Test
  void forAddressOverloadAcceptsAddressInstance() {
    Address addr = new Address(FN, "x");
    Message m = MessageBuilder.forAddress(addr).withValue(1L).build();

    assertThat(m.targetAddress()).isEqualTo(addr);
  }

  @Test
  void forAddressRejectsNullAddress() {
    assertThatThrownBy(() -> MessageBuilder.forAddress((Address) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void withTargetAddressOverridesPreviouslySetAddress() {
    Address newTarget = new Address(TypeName.typeNameOf("other", "fn"), "id-2");

    Message m =
        MessageBuilder.forAddress(FN, "id-1")
            .withValue(1L)
            .withTargetAddress(newTarget)
            .build();

    assertThat(m.targetAddress()).isEqualTo(newTarget);
  }

  @Test
  void withTargetAddressTypeNameAndIdOverloadDelegates() {
    TypeName otherType = TypeName.typeNameOf("other", "fn");

    Message m =
        MessageBuilder.forAddress(FN, "id-1")
            .withValue(1L)
            .withTargetAddress(otherType, "id-2")
            .build();

    assertThat(m.targetAddress()).isEqualTo(new Address(otherType, "id-2"));
  }

  @Test
  void withCustomTypeRejectsNullType() {
    MessageBuilder builder = MessageBuilder.forAddress(FN, "id-1");
    assertThatThrownBy(() -> builder.withCustomType(null, "x"))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void withCustomTypeRejectsNullElement() {
    MessageBuilder builder = MessageBuilder.forAddress(FN, "id-1");
    assertThatThrownBy(() -> builder.withCustomType(Types.stringType(), null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void fromMessageProducesEqualMessage() {
    Message original = MessageBuilder.forAddress(FN, "id-1").withValue("payload").build();

    Message rebuilt = MessageBuilder.fromMessage(original).build();

    assertThat(rebuilt).isEqualTo(original);
  }

  @Test
  void fromMessageAllowsTargetAddressOverride() {
    Message original = MessageBuilder.forAddress(FN, "id-1").withValue("payload").build();
    Address newTarget = new Address(TypeName.typeNameOf("other", "fn"), "id-2");

    Message rebuilt =
        MessageBuilder.fromMessage(original).withTargetAddress(newTarget).build();

    assertThat(rebuilt.targetAddress()).isEqualTo(newTarget);
    assertThat(rebuilt.asUtf8String()).isEqualTo("payload");
  }

  @Test
  void messageWrapperRejectsTypedValueWithoutHasValue() {
    Address addr = new Address(FN, "id-1");
    TypedValue empty = TypedValue.newBuilder().build();

    assertThatThrownBy(() -> new MessageWrapper(addr, empty))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unset empty Messages are prohibited");
  }

  @Test
  void equalsAndHashCodeRespectAddressAndPayload() {
    Message a = MessageBuilder.forAddress(FN, "id-1").withValue("v").build();
    Message b = MessageBuilder.forAddress(FN, "id-1").withValue("v").build();
    Message differentAddress = MessageBuilder.forAddress(FN, "id-2").withValue("v").build();
    Message differentValue = MessageBuilder.forAddress(FN, "id-1").withValue("w").build();

    assertThat(a)
        .isEqualTo(b)
        .hasSameHashCodeAs(b)
        .isNotEqualTo(differentAddress)
        .isNotEqualTo(differentValue);
  }

  @Test
  void equalsToSelfAndNotToNullOrUnrelated() {
    Message m = MessageBuilder.forAddress(FN, "id-1").withValue("v").build();
    assertThat(m).isEqualTo(m).isNotNull().isNotEqualTo("string");
  }

  @Test
  void toStringIncludesTargetAddress() {
    // Implementation has a toString that's used in error logs — pin presence of address.
    Message m = MessageBuilder.forAddress(FN, "id-xyz").withValue(1L).build();
    assertThat(m.toString()).contains("id-xyz");
  }
}
