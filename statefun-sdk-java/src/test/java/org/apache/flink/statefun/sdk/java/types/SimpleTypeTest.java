// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.sdk.java.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.junit.jupiter.api.Test;

public class SimpleTypeTest {

  @Test
  public void mutableType() {
    final Type<String> type =
        SimpleType.simpleTypeFrom(
            TypeName.typeNameFromString("test/simple-mutable-type"),
            val -> val.getBytes(StandardCharsets.UTF_8),
            bytes -> new String(bytes, StandardCharsets.UTF_8));

    assertThat(type.typeName(), is(TypeName.typeNameFromString("test/simple-mutable-type")));
    assertRoundTrip(type, "hello world!");
  }

  @Test
  public void immutableType() {
    final Type<String> type =
        SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("test/simple-immutable-type"),
            val -> val.getBytes(StandardCharsets.UTF_8),
            bytes -> new String(bytes, StandardCharsets.UTF_8));

    assertThat(type.typeName(), is(TypeName.typeNameFromString("test/simple-immutable-type")));
    assertRoundTrip(type, "hello world!");
  }

  public <T> void assertRoundTrip(Type<T> type, T element) {
    final Slice slice;
    {
      TypeSerializer<T> serializer = type.typeSerializer();
      slice = serializer.serialize(element);
    }
    TypeSerializer<T> serializer = type.typeSerializer();
    T deserialized = serializer.deserialize(slice);
    assertEquals(element, deserialized);
  }
}
