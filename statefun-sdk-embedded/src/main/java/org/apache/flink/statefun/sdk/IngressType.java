// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk;

import java.util.Objects;
import org.apache.flink.statefun.sdk.io.IngressSpec;

/**
 * Defines the type of an ingress, represented by a namespace and the type's name.
 *
 * <p>This is used by the system to translate an {@link IngressSpec} to a physical runtime-specific
 * representation.
 */
public final class IngressType {
  private final String namespace;
  private final String type;

  /**
   * Creates an {@link IngressType}.
   *
   * @param namespace the type's namespace.
   * @param type the type's name.
   */
  public IngressType(String namespace, String type) {
    this.namespace = Objects.requireNonNull(namespace);
    this.type = Objects.requireNonNull(type);
  }

  /**
   * Returns the namespace of this ingress type.
   *
   * @return the namespace of this ingress type.
   */
  public String namespace() {
    return namespace;
  }

  /**
   * Returns the name of this ingress type.
   *
   * @return the name of this ingress type.
   */
  public String type() {
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
    IngressType that = (IngressType) o;
    return namespace.equals(that.namespace) && type.equals(that.type);
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
    return String.format("IngressType(%s, %s)", namespace, type);
  }
}
