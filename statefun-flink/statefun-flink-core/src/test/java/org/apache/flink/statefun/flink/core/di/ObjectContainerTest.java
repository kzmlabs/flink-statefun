// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.di;

import static org.hamcrest.CoreMatchers.theInstance;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.Test;

public class ObjectContainerTest {

  @Test
  public void addAliasTest() {
    final ObjectContainer container = new ObjectContainer();

    container.add("label-1", InterfaceA.class, TestClass.class);
    container.addAlias("label-2", InterfaceB.class, "label-1", InterfaceA.class);

    assertThat(
        container.get(InterfaceB.class, "label-2"),
        theInstance(container.get(InterfaceA.class, "label-1")));
  }

  private interface InterfaceA {}

  private interface InterfaceB {}

  private static class TestClass implements InterfaceA, InterfaceB {}
}
