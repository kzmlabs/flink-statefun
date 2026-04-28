// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.sdk.java.storage;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.TypeSerializer;

public class TestMutableType implements Type<TestMutableType.Type> {

  @Override
  public TypeName typeName() {
    return TypeName.typeNameOf("test", "my-mutable-type");
  }

  @Override
  public TypeSerializer<TestMutableType.Type> typeSerializer() {
    return new Serializer();
  }

  public static class Type {
    private String value;

    public Type(String value) {
      this.value = value;
    }

    public void mutate(String newValue) {
      this.value = newValue;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Type type = (Type) o;
      return Objects.equals(value, type.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }
  }

  private static class Serializer implements TypeSerializer<TestMutableType.Type> {
    @Override
    public Slice serialize(Type value) {
      return Slices.wrap(value.value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Type deserialize(Slice bytes) {
      return new Type(new String(bytes.toByteArray(), StandardCharsets.UTF_8));
    }
  }
}
