// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.types.remote;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.statefun.sdk.TypeName;
import org.junit.jupiter.api.Test;

class RemoteValueSerializerTest {

  private static final TypeName TYPE = new TypeName("io.test", "remote-value");
  private static final TypeName OTHER_TYPE = new TypeName("io.test", "other-value");

  @Test
  void roundtripPreservesByteContent() throws Exception {
    RemoteValueSerializer serializer = new RemoteValueSerializer(TYPE);
    byte[] payload = new byte[] {1, 2, 3, (byte) 0xFF, 0, 7};

    byte[] roundtripped = serializeThenDeserialize(serializer, payload);

    assertThat(roundtripped).containsExactly(payload);
  }

  @Test
  void roundtripWithEmptyPayload() throws Exception {
    RemoteValueSerializer serializer = new RemoteValueSerializer(TYPE);

    byte[] roundtripped = serializeThenDeserialize(serializer, new byte[0]);

    assertThat(roundtripped).isEmpty();
  }

  @Test
  void serializeRejectsNullRecord() {
    RemoteValueSerializer serializer = new RemoteValueSerializer(TYPE);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(new DataOutputStream(out));

    assertThatThrownBy(() -> serializer.serialize(null, view))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be null");
  }

  @Test
  void copyByteArrayProducesIndependentArrayWithSameContent() {
    RemoteValueSerializer serializer = new RemoteValueSerializer(TYPE);
    byte[] original = new byte[] {10, 20, 30};

    byte[] copy = serializer.copy(original);
    assertThat(copy).isNotSameAs(original).containsExactly(original);

    // Mutate original — copy must be independent.
    original[0] = (byte) 0xFF;
    assertThat(copy[0]).isEqualTo((byte) 10);
  }

  @Test
  void copyWithReuseDelegatesToCopy() {
    RemoteValueSerializer serializer = new RemoteValueSerializer(TYPE);
    byte[] original = new byte[] {1, 2};
    byte[] reuse = new byte[] {99}; // ignored
    byte[] copy = serializer.copy(original, reuse);

    assertThat(copy).isNotSameAs(original).containsExactly(original);
  }

  @Test
  void deserializeWithReuseIgnoresReuseAndReturnsFreshArray() throws Exception {
    RemoteValueSerializer serializer = new RemoteValueSerializer(TYPE);
    byte[] payload = new byte[] {7, 8, 9};

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    serializer.serialize(payload, new DataOutputViewStreamWrapper(new DataOutputStream(out)));

    DataInputViewStreamWrapper in =
        new DataInputViewStreamWrapper(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    byte[] reuse = new byte[] {0};
    byte[] result = serializer.deserialize(reuse, in);

    assertThat(result).containsExactly(payload);
  }

  @Test
  void copyDataInputToDataOutputCopiesByteRange() throws Exception {
    RemoteValueSerializer serializer = new RemoteValueSerializer(TYPE);
    byte[] payload = new byte[] {1, 2, 3, 4, 5};

    ByteArrayOutputStream serializedOut = new ByteArrayOutputStream();
    serializer.serialize(
        payload, new DataOutputViewStreamWrapper(new DataOutputStream(serializedOut)));

    ByteArrayOutputStream copyOut = new ByteArrayOutputStream();
    serializer.copy(
        new DataInputViewStreamWrapper(
            new DataInputStream(new ByteArrayInputStream(serializedOut.toByteArray()))),
        new DataOutputViewStreamWrapper(new DataOutputStream(copyOut)));

    // copyDataInputToDataOutput should produce a byte-identical re-serialized payload.
    assertThat(copyOut.toByteArray()).isEqualTo(serializedOut.toByteArray());
  }

  @Test
  void duplicateProducesEqualButIndependentSerializer() {
    RemoteValueSerializer serializer = new RemoteValueSerializer(TYPE);
    TypeSerializer<byte[]> dup = serializer.duplicate();

    assertThat(dup).isNotSameAs(serializer).isEqualTo(serializer);
  }

  @Test
  void equalsAndHashCodeContract() {
    RemoteValueSerializer s1 = new RemoteValueSerializer(TYPE);
    RemoteValueSerializer s2 = new RemoteValueSerializer(TYPE);
    RemoteValueSerializer different = new RemoteValueSerializer(OTHER_TYPE);

    assertThat(s1).isEqualTo(s1).isEqualTo(s2);
    assertThat(s1.hashCode()).isEqualTo(s2.hashCode());
    assertThat(s1).isNotEqualTo(different).isNotEqualTo(null).isNotEqualTo("foo");
  }

  @Test
  void simpleAccessorsReturnConsistentValues() {
    RemoteValueSerializer serializer = new RemoteValueSerializer(TYPE);

    assertThat(serializer.getType()).isEqualTo(TYPE);
    assertThat(serializer.isImmutableType()).isFalse();
    assertThat(serializer.getLength()).isEqualTo(-1);
    assertThat(serializer.createInstance()).isEmpty();
  }

  @Test
  void constructorRejectsNullType() {
    assertThatThrownBy(() -> new RemoteValueSerializer(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void snapshotConfigurationCarriesType() {
    RemoteValueSerializer serializer = new RemoteValueSerializer(TYPE);
    RemoteValueSerializerSnapshot snapshot =
        (RemoteValueSerializerSnapshot) serializer.snapshotConfiguration();

    assertThat(snapshot.type()).isEqualTo(TYPE);
  }

  @Test
  void snapshotRoundtripsTypeNamesViaWriteAndRead() throws Exception {
    RemoteValueSerializerSnapshot original = new RemoteValueSerializerSnapshot(TYPE);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    original.writeSnapshot(new DataOutputViewStreamWrapper(new DataOutputStream(out)));

    RemoteValueSerializerSnapshot restored = new RemoteValueSerializerSnapshot();
    restored.readSnapshot(
        original.getCurrentVersion(),
        new DataInputViewStreamWrapper(new DataInputStream(new ByteArrayInputStream(out.toByteArray()))),
        getClass().getClassLoader());

    assertThat(restored.type()).isEqualTo(TYPE);
  }

  @Test
  void snapshotCurrentVersionIsOne() {
    assertThat(new RemoteValueSerializerSnapshot().getCurrentVersion()).isEqualTo(1);
  }

  @Test
  void restoreSerializerProducesEquivalentSerializer() {
    RemoteValueSerializerSnapshot snapshot = new RemoteValueSerializerSnapshot(TYPE);
    TypeSerializer<byte[]> restored = snapshot.restoreSerializer();

    assertThat(restored).isInstanceOf(RemoteValueSerializer.class);
    assertThat(((RemoteValueSerializer) restored).getType()).isEqualTo(TYPE);
  }

  @Test
  void resolveSchemaCompatibilityWithSameTypeIsCompatible() {
    RemoteValueSerializerSnapshot a = new RemoteValueSerializerSnapshot(TYPE);
    RemoteValueSerializerSnapshot b = new RemoteValueSerializerSnapshot(TYPE);

    TypeSerializerSchemaCompatibility<byte[]> result = a.resolveSchemaCompatibility(b);

    assertThat(result.isCompatibleAsIs()).isTrue();
  }

  @Test
  void resolveSchemaCompatibilityWithDifferentTypeThrows() {
    RemoteValueSerializerSnapshot a = new RemoteValueSerializerSnapshot(TYPE);
    RemoteValueSerializerSnapshot b = new RemoteValueSerializerSnapshot(OTHER_TYPE);

    assertThatThrownBy(() -> a.resolveSchemaCompatibility(b))
        .isInstanceOf(RemoteValueTypeMismatchException.class)
        .hasMessageContaining(TYPE.toString())
        .hasMessageContaining(OTHER_TYPE.toString());
  }

  @Test
  void resolveSchemaCompatibilityWithDifferentSnapshotTypeIsIncompatible() {
    RemoteValueSerializerSnapshot snapshot = new RemoteValueSerializerSnapshot(TYPE);

    @SuppressWarnings({"rawtypes", "unchecked"})
    TypeSerializerSchemaCompatibility<byte[]> result =
        snapshot.resolveSchemaCompatibility(new ForeignSnapshot());

    assertThat(result.isIncompatible()).isTrue();
  }

  private static byte[] serializeThenDeserialize(RemoteValueSerializer serializer, byte[] payload)
      throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    serializer.serialize(payload, new DataOutputViewStreamWrapper(new DataOutputStream(out)));
    return serializer.deserialize(
        new DataInputViewStreamWrapper(
            new DataInputStream(new ByteArrayInputStream(out.toByteArray()))));
  }

  /** Test double: an unrelated TypeSerializerSnapshot subtype. */
  private static final class ForeignSnapshot
      implements org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<byte[]> {
    @Override
    public int getCurrentVersion() {
      return 1;
    }

    @Override
    public void writeSnapshot(org.apache.flink.core.memory.DataOutputView dataOutputView) {}

    @Override
    public void readSnapshot(
        int i, org.apache.flink.core.memory.DataInputView dataInputView, ClassLoader classLoader) {}

    @Override
    public TypeSerializer<byte[]> restoreSerializer() {
      return null;
    }

    @Override
    public TypeSerializerSchemaCompatibility<byte[]> resolveSchemaCompatibility(
        org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<byte[]> other) {
      return TypeSerializerSchemaCompatibility.incompatible();
    }
  }
}
