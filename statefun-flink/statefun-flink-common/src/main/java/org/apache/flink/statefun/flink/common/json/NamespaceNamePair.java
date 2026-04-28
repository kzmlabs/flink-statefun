// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.common.json;

import java.util.Objects;

public final class NamespaceNamePair {
  private final String namespace;
  private final String name;

  public static NamespaceNamePair from(String namespaceAndName) {
    Objects.requireNonNull(namespaceAndName);
    final int pos = namespaceAndName.lastIndexOf("/");
    if (pos <= 0 || pos == namespaceAndName.length() - 1) {
      throw new IllegalArgumentException(
          namespaceAndName + " does not conform to the <namespace>/<name> format");
    }
    String namespace = namespaceAndName.substring(0, pos);
    String name = namespaceAndName.substring(pos + 1);
    return new NamespaceNamePair(namespace, name);
  }

  private NamespaceNamePair(String namespace, String name) {
    this.namespace = Objects.requireNonNull(namespace);
    this.name = Objects.requireNonNull(name);
  }

  public String namespace() {
    return namespace;
  }

  public String name() {
    return name;
  }
}
