// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.sdk;

import java.io.Serializable;
import java.util.Objects;

public final class TypeName implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final String DELIMITER = "/";

  private final String namespace;
  private final String name;
  private final String canonicalTypenameString;

  public static TypeName parseFrom(String typeNameString) {
    final String[] split = typeNameString.split(DELIMITER);
    if (split.length != 2) {
      throw new IllegalArgumentException(
          "Invalid type name string: "
              + typeNameString
              + ". Must be of format <namespace>"
              + DELIMITER
              + "<name>.");
    }
    return new TypeName(split[0], split[1]);
  }

  public TypeName(String namespace, String name) {
    this.namespace = Objects.requireNonNull(namespace);
    this.name = Objects.requireNonNull(name);
    this.canonicalTypenameString = canonicalTypeNameString(namespace, name);
  }

  public String namespace() {
    return namespace;
  }

  public String name() {
    return name;
  }

  public String canonicalTypenameString() {
    return canonicalTypenameString;
  }

  @Override
  public String toString() {
    return "TypeName(" + namespace + ", " + name + ")";
  }

  private static String canonicalTypeNameString(String namespace, String name) {
    return namespace + DELIMITER + name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TypeName typeName = (TypeName) o;
    return Objects.equals(namespace, typeName.namespace)
        && Objects.equals(name, typeName.name)
        && Objects.equals(canonicalTypenameString, typeName.canonicalTypenameString);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, name, canonicalTypenameString);
  }
}
