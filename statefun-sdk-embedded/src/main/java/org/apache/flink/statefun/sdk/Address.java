// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk;

import java.util.Objects;

/**
 * An {@link Address} is the unique identity of an individual {@link StatefulFunction}, containing
 * of the function's {@link FunctionType} and an unique identifier within the type. The function's
 * type denotes the class of function to invoke, while the unique identifier addresses the
 * invocation to a specific function instance.
 */
public final class Address {
  private final FunctionType type;
  private final String id;

  /**
   * Creates an {@link Address}.
   *
   * @param type type of the function.
   * @param id unique id within the function type.
   */
  public Address(FunctionType type, String id) {
    this.type = Objects.requireNonNull(type);
    this.id = Objects.requireNonNull(id);
  }

  /**
   * Returns the {@link FunctionType} that this address identifies.
   *
   * @return type of the function
   */
  public FunctionType type() {
    return type;
  }

  /**
   * Returns the unique function id, within its type, that this address identifies.
   *
   * @return unique id within the function type.
   */
  public String id() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Address address = (Address) o;
    return type.equals(address.type) && id.equals(address.id);
  }

  @Override
  public int hashCode() {
    int hash = 0;
    hash = 37 * hash + type.hashCode();
    hash = 37 * hash + id.hashCode();
    return hash;
  }

  @Override
  public String toString() {
    return String.format("Address(%s, %s, %s)", type.namespace(), type.name(), id);
  }
}
