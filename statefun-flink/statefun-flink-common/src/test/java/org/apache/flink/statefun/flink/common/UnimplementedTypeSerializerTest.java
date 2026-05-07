// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class UnimplementedTypeSerializerTest {

  /**
   * The serializer is intentionally a placeholder: every method except equals/hashCode/getLength
   * must throw to make sure no caller silently exercises a half-implementation. Pin that
   * behavior so a future contributor doesn't accidentally implement one method without the rest.
   */
  @Test
  void allOperationalMethodsThrowUnsupported() {
    UnimplementedTypeSerializer<String> s = new UnimplementedTypeSerializer<>();

    assertThatThrownBy(() -> s.duplicate()).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> s.createInstance()).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> s.copy("a")).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> s.copy("a", "b")).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> s.serialize("a", null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> s.deserialize((org.apache.flink.core.memory.DataInputView) null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> s.deserialize("a", null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(
            () ->
                s.copy(
                    (org.apache.flink.core.memory.DataInputView) null,
                    (org.apache.flink.core.memory.DataOutputView) null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> s.snapshotConfiguration())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void shapeAccessorsReturnConstants() {
    UnimplementedTypeSerializer<String> s = new UnimplementedTypeSerializer<>();

    // The serializer claims a fixed length of 0 (sentinel for "I'm a stub") and not-immutable.
    assertThat(s.getLength()).isZero();
    assertThat(s.isImmutableType()).isFalse();
  }

  @Test
  void equalsIsIdentityNotValueBased() {
    UnimplementedTypeSerializer<String> a = new UnimplementedTypeSerializer<>();
    UnimplementedTypeSerializer<String> b = new UnimplementedTypeSerializer<>();

    // Two instances are NOT equal — the contract is reference equality, otherwise Flink would
    // try to merge two unrelated stub serializers.
    assertThat(a).isEqualTo(a);
    assertThat(a).isNotEqualTo(b);
    assertThat(a).isNotEqualTo("not a serializer");
    assertThat(a).isNotEqualTo(null);
  }

  @Test
  void hashCodeIsConstantSeven() {
    // Pin this — `7` is documented as the placeholder hash. Any change to a real hash would
    // imply someone is treating this as a real serializer.
    assertThat(new UnimplementedTypeSerializer<String>().hashCode()).isEqualTo(7);
  }
}
