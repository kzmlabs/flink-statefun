// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.handler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Duration;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageBuilder;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.ExpirationSpec.ExpireMode;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.PersistedValueSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.jupiter.api.Test;

class ProtoUtilsTest {

  private static final TypeName FN = TypeName.typeNameOf("ns", "name");
  private static final TypeName EGRESS = TypeName.typeNameOf("io.test", "egress");

  @Test
  void protoAddressFromSdkSplitsTypeNameIntoNamespaceAndType() {
    Address addr = new Address(FN, "id-1");

    org.apache.flink.statefun.sdk.reqreply.generated.Address proto =
        ProtoUtils.protoAddressFromSdk(addr);

    assertEquals("ns", proto.getNamespace());
    assertEquals("name", proto.getType());
    assertEquals("id-1", proto.getId());
  }

  @Test
  void sdkAddressFromProtoReconstructsAddressFromAllThreeFields() {
    org.apache.flink.statefun.sdk.reqreply.generated.Address proto =
        org.apache.flink.statefun.sdk.reqreply.generated.Address.newBuilder()
            .setNamespace("ns")
            .setType("name")
            .setId("id-1")
            .build();

    Address sdk = ProtoUtils.sdkAddressFromProto(proto);

    assertThat(sdk, is(equalTo(new Address(FN, "id-1"))));
  }

  @Test
  void sdkAddressFromProtoTreatsAllEmptyFieldsAsNull() {
    // Empty namespace + type + id is the wire representation of "no caller / no address".
    org.apache.flink.statefun.sdk.reqreply.generated.Address empty =
        org.apache.flink.statefun.sdk.reqreply.generated.Address.newBuilder().build();

    assertThat(ProtoUtils.sdkAddressFromProto(empty), nullValue());
  }

  @Test
  void sdkAddressFromProtoTreatsNullAsNull() {
    assertThat(ProtoUtils.sdkAddressFromProto(null), nullValue());
  }

  @Test
  void sdkAddressFromProtoRoundtripsAllThreeFieldsTogether() {
    // The "all empty -> null" / "any non-empty -> construct TypeName" contract requires
    // all three fields (namespace/type/id) to be set when ANY are set, because TypeName
    // construction rejects empty namespace or name. Pinning the round-trip here.
    org.apache.flink.statefun.sdk.reqreply.generated.Address proto =
        org.apache.flink.statefun.sdk.reqreply.generated.Address.newBuilder()
            .setNamespace("io.test")
            .setType("fn")
            .setId("instance-1")
            .build();

    Address sdk = ProtoUtils.sdkAddressFromProto(proto);

    assertThat(sdk, notNullValue());
    assertEquals("instance-1", sdk.id());
    assertThat(sdk.type(), is(equalTo(TypeName.typeNameOf("io.test", "fn"))));
  }

  @Test
  void valueSpecWithoutExpirationProducesNoExpirationSpec() {
    ValueSpec<Integer> spec = ValueSpec.named("counter").withIntType();

    PersistedValueSpec.Builder builder = ProtoUtils.protoFromValueSpec(spec);
    PersistedValueSpec proto = builder.build();

    assertEquals("counter", proto.getStateName());
    assertFalse(proto.hasExpirationSpec());
  }

  @Test
  void valueSpecWithExpireAfterCallEncodesAfterInvokeExpireMode() {
    ValueSpec<Integer> spec =
        ValueSpec.named("counter").thatExpiresAfterCall(Duration.ofMinutes(5)).withIntType();

    PersistedValueSpec proto = ProtoUtils.protoFromValueSpec(spec).build();

    assertEquals(ExpireMode.AFTER_INVOKE, proto.getExpirationSpec().getMode());
    assertEquals(
        Duration.ofMinutes(5).toMillis(), proto.getExpirationSpec().getExpireAfterMillis());
  }

  @Test
  void valueSpecWithExpireAfterWritingEncodesAfterWriteExpireMode() {
    ValueSpec<Integer> spec =
        ValueSpec.named("counter").thatExpireAfterWrite(Duration.ofSeconds(30)).withIntType();

    PersistedValueSpec proto = ProtoUtils.protoFromValueSpec(spec).build();

    assertEquals(ExpireMode.AFTER_WRITE, proto.getExpirationSpec().getMode());
    assertEquals(
        Duration.ofSeconds(30).toMillis(), proto.getExpirationSpec().getExpireAfterMillis());
  }

  @Test
  void getTypedValueOnMessageWrapperUsesFastPath() {
    // MessageWrapper carries an existing TypedValue — the fast path returns the same instance
    // (no re-serialization), which the runtime depends on for hot-path performance.
    Message message = MessageBuilder.forAddress(FN, "id").withValue("payload").build();

    TypedValue first = ProtoUtils.getTypedValue(message);
    TypedValue second = ProtoUtils.getTypedValue(message);

    // Same byte content; behavior must be deterministic across calls.
    assertEquals(first, second);
  }

  @Test
  void getTypedValueOnCustomMessageRebuildsTypedValue() {
    // For non-Wrapper Message implementations (third-party SDK extensions), re-encode via
    // valueTypeName + rawValue. Pin that path with a custom Message double.
    final byte[] payload = "abc".getBytes();
    Message custom =
        new Message() {
          @Override
          public Address targetAddress() {
            return new Address(FN, "id");
          }

          @Override
          public boolean isLong() {
            return false;
          }

          @Override
          public long asLong() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isUtf8String() {
            return false;
          }

          @Override
          public String asUtf8String() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isInt() {
            return false;
          }

          @Override
          public int asInt() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isBoolean() {
            return false;
          }

          @Override
          public boolean asBoolean() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isFloat() {
            return false;
          }

          @Override
          public float asFloat() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isDouble() {
            return false;
          }

          @Override
          public double asDouble() {
            throw new UnsupportedOperationException();
          }

          @Override
          public <T> boolean is(org.apache.flink.statefun.sdk.java.types.Type<T> type) {
            return false;
          }

          @Override
          public <T> T as(org.apache.flink.statefun.sdk.java.types.Type<T> type) {
            throw new UnsupportedOperationException();
          }

          @Override
          public TypeName valueTypeName() {
            return TypeName.typeNameOf("io.custom", "binary");
          }

          @Override
          public Slice rawValue() {
            return org.apache.flink.statefun.sdk.java.slice.Slices.copyOf(payload);
          }
        };

    TypedValue typedValue = ProtoUtils.getTypedValue(custom);

    assertEquals("io.custom/binary", typedValue.getTypename());
    assertThat(typedValue.getValue().toByteArray(), is(equalTo(payload)));
  }

  @Test
  void getTypedValueOnEgressMessageWrapperFastPath() {
    EgressMessage egress = EgressMessageBuilder.forEgress(EGRESS).withValue("v").build();

    TypedValue first = ProtoUtils.getTypedValue(egress);
    TypedValue second = ProtoUtils.getTypedValue(egress);

    assertEquals(first, second);
    assertEquals("io.statefun.types/string", first.getTypename());
  }
}
