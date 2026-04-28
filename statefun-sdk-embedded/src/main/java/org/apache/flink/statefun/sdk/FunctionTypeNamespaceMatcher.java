// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.sdk;

import java.io.Serializable;
import java.util.Objects;

public final class FunctionTypeNamespaceMatcher implements Serializable {

  private static final long serialVersionUID = 1;

  private final String targetNamespace;

  public static FunctionTypeNamespaceMatcher targetNamespace(String namespace) {
    return new FunctionTypeNamespaceMatcher(namespace);
  }

  private FunctionTypeNamespaceMatcher(String targetNamespace) {
    this.targetNamespace = Objects.requireNonNull(targetNamespace);
  }

  public String targetNamespace() {
    return targetNamespace;
  }

  public boolean matches(FunctionType functionType) {
    return targetNamespace.equals(functionType.namespace());
  }

  @Override
  public int hashCode() {
    return targetNamespace.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    FunctionTypeNamespaceMatcher other = (FunctionTypeNamespaceMatcher) obj;
    return targetNamespace.equals(other.targetNamespace);
  }

  @Override
  public String toString() {
    return String.format("FunctionTypeNamespaceMatcher(%s)", targetNamespace);
  }
}
