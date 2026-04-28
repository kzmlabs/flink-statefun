// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk;

import java.io.Serializable;
import java.util.Objects;

/**
 * This class represents the type of a {@link StatefulFunction}, consisting of a namespace of the
 * function type as well as the type's name.
 *
 * <p>A function's type is part of a function's {@link Address} and serves as integral part of an
 * individual function's identity.
 *
 * @see Address
 */
public final class FunctionType implements Serializable {

  private static final long serialVersionUID = 1;

  private final String namespace;
  private final String type;

  /**
   * Creates a {@link FunctionType}.
   *
   * @param namespace the function type's namepsace.
   * @param type the function type's name.
   */
  public FunctionType(String namespace, String type) {
    this.namespace = Objects.requireNonNull(namespace);
    this.type = Objects.requireNonNull(type);
  }

  /**
   * Returns the namespace of the function type.
   *
   * @return the namespace of the function type.
   */
  public String namespace() {
    return namespace;
  }

  /**
   * Returns the name of the function type.
   *
   * @return the name of the function type.
   */
  public String name() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FunctionType functionType = (FunctionType) o;
    return namespace.equals(functionType.namespace) && type.equals(functionType.type);
  }

  @Override
  public int hashCode() {
    int hash = 0;
    hash = 37 * hash + namespace.hashCode();
    hash = 37 * hash + type.hashCode();
    return hash;
  }

  @Override
  public String toString() {
    return String.format("FunctionType(%s, %s)", namespace, type);
  }
}
