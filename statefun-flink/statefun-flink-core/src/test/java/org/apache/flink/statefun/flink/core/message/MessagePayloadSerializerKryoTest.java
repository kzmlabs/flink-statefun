// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.message;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.generated.Payload;
import org.junit.jupiter.api.Test;

class MessagePayloadSerializerKryoTest {

  private final MessagePayloadSerializerKryo serializer = new MessagePayloadSerializerKryo();
  private final ClassLoader cl = getClass().getClassLoader();

  @Test
  void roundtripPreservesSimplePojo() {
    Pojo original = new Pojo("hello", 42, new int[] {1, 2, 3});

    Payload serialized = serializer.serialize(original);
    Object deserialized = serializer.deserialize(cl, serialized);

    assertThat(deserialized).isInstanceOf(Pojo.class);
    Pojo restored = (Pojo) deserialized;
    assertThat(restored.name).isEqualTo("hello");
    assertThat(restored.count).isEqualTo(42);
    assertThat(restored.values).containsExactly(1, 2, 3);
  }

  @Test
  void serializedPayloadCarriesClassNameForCrossClassloaderReconstruction() {
    Pojo original = new Pojo("p", 7, new int[] {});

    Payload serialized = serializer.serialize(original);

    assertThat(serialized.getClassName()).isEqualTo(Pojo.class.getName());
    assertThat(serialized.getPayloadBytes().size()).isPositive();
  }

  @Test
  void copyProducesEqualButIndependentInstance() {
    Pojo original = new Pojo("c", 9, new int[] {7});

    Object copy = serializer.copy(cl, original);

    assertThat(copy).isNotSameAs(original).isInstanceOf(Pojo.class);
    Pojo restored = (Pojo) copy;
    assertThat(restored.name).isEqualTo("c");
    assertThat(restored.count).isEqualTo(9);
    assertThat(restored.values).containsExactly(7);
    // Mutate original — copy must be independent.
    original.values[0] = 99;
    assertThat(restored.values).containsExactly(7);
  }

  @Test
  void roundtripsNullableFieldAsNull() {
    Pojo original = new Pojo(null, 0, new int[0]);

    Payload serialized = serializer.serialize(original);
    Pojo restored = (Pojo) serializer.deserialize(cl, serialized);

    assertThat(restored.name).isNull();
  }

  @Test
  void multipleRoundtripsReuseInternalBuffersWithoutCorruption() {
    // Pin: the serializer reuses target/source buffers across calls (clear()/setBuffer()).
    // Multiple roundtrips must not bleed state between invocations.
    for (int i = 0; i < 50; i++) {
      Pojo original = new Pojo("iter-" + i, i, new int[] {i, i * 2});
      Payload serialized = serializer.serialize(original);
      Pojo restored = (Pojo) serializer.deserialize(cl, serialized);
      assertThat(restored.name).isEqualTo("iter-" + i);
      assertThat(restored.count).isEqualTo(i);
      assertThat(restored.values).containsExactly(i, i * 2);
    }
  }

  /** A minimal Kryo-friendly POJO. */
  public static final class Pojo implements Serializable {
    private static final long serialVersionUID = 1L;
    public String name;
    public int count;
    public int[] values;

    public Pojo() {}

    public Pojo(String name, int count, int[] values) {
      this.name = name;
      this.count = count;
      this.values = values;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Pojo)) return false;
      Pojo p = (Pojo) o;
      return count == p.count && Objects.equals(name, p.name) && Arrays.equals(values, p.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, count, Arrays.hashCode(values));
    }
  }
}
