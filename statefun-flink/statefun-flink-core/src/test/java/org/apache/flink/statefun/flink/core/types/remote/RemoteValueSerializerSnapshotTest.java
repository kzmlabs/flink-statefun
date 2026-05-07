// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.types.remote;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.statefun.sdk.TypeName;
import org.junit.jupiter.api.Test;

class RemoteValueSerializerSnapshotTest {

  private static final TypeName TYPE = new TypeName("io.test", "remote");
  private static final TypeName OTHER = new TypeName("io.test", "other");

  @Test
  void writeAndReadSnapshotRoundtripsTypeName() throws Exception {
    RemoteValueSerializerSnapshot snapshot = new RemoteValueSerializerSnapshot(TYPE);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    snapshot.writeSnapshot(new DataOutputViewStreamWrapper(new DataOutputStream(out)));

    RemoteValueSerializerSnapshot restored = new RemoteValueSerializerSnapshot();
    restored.readSnapshot(
        snapshot.getCurrentVersion(),
        new DataInputViewStreamWrapper(new DataInputStream(new ByteArrayInputStream(out.toByteArray()))),
        getClass().getClassLoader());

    assertThat(restored.type()).isEqualTo(TYPE);
  }

  @Test
  void restoreSerializerProducesRemoteValueSerializer() {
    RemoteValueSerializerSnapshot snapshot = new RemoteValueSerializerSnapshot(TYPE);

    RemoteValueSerializer restored = (RemoteValueSerializer) snapshot.restoreSerializer();

    assertThat(restored.getType()).isEqualTo(TYPE);
  }

  @Test
  void resolveCompatibilityWithSelfIsCompatibleAsIs() {
    RemoteValueSerializerSnapshot snapshot = new RemoteValueSerializerSnapshot(TYPE);

    TypeSerializerSchemaCompatibility<byte[]> result = snapshot.resolveSchemaCompatibility(snapshot);

    assertThat(result.isCompatibleAsIs()).isTrue();
  }

  @Test
  void resolveCompatibilityWithDifferentTypeRaisesMismatch() {
    RemoteValueSerializerSnapshot mine = new RemoteValueSerializerSnapshot(TYPE);
    RemoteValueSerializerSnapshot theirs = new RemoteValueSerializerSnapshot(OTHER);

    // Cross-type restore must surface a hard failure with both type names so operators
    // can diagnose the schema drift instead of silently dropping data.
    assertThatThrownBy(() -> mine.resolveSchemaCompatibility(theirs))
        .isInstanceOf(RemoteValueTypeMismatchException.class);
  }

  @Test
  void resolveCompatibilityRejectsForeignSnapshotType() {
    RemoteValueSerializerSnapshot mine = new RemoteValueSerializerSnapshot(TYPE);

    @SuppressWarnings("unchecked")
    TypeSerializerSnapshot<byte[]> foreign =
        (TypeSerializerSnapshot<byte[]>) (TypeSerializerSnapshot<?>) new ForeignSnapshot();

    TypeSerializerSchemaCompatibility<byte[]> result = mine.resolveSchemaCompatibility(foreign);

    assertThat(result.isIncompatible()).isTrue();
  }

  @Test
  void currentVersionIsOne() {
    assertThat(new RemoteValueSerializerSnapshot(TYPE).getCurrentVersion()).isEqualTo(1);
  }

  /** A non-RemoteValue snapshot used to exercise the foreign-snapshot guard. */
  private static final class ForeignSnapshot implements TypeSerializerSnapshot<Object> {
    @Override
    public int getCurrentVersion() {
      return 0;
    }

    @Override
    public void writeSnapshot(org.apache.flink.core.memory.DataOutputView out) {}

    @Override
    public void readSnapshot(int v, org.apache.flink.core.memory.DataInputView in, ClassLoader cl) {}

    @Override
    public org.apache.flink.api.common.typeutils.TypeSerializer<Object> restoreSerializer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TypeSerializerSchemaCompatibility<Object> resolveSchemaCompatibility(
        TypeSerializerSnapshot<Object> other) {
      return TypeSerializerSchemaCompatibility.incompatible();
    }
  }
}
