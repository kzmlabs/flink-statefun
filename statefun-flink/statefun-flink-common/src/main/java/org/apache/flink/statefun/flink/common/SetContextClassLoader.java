// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.common;

import java.io.Closeable;
import javax.annotation.Nonnull;

public final class SetContextClassLoader implements Closeable {
  private final ClassLoader originalClassLoader;

  public SetContextClassLoader(@Nonnull Object o) {
    this(o.getClass().getClassLoader());
  }

  public SetContextClassLoader(@Nonnull ClassLoader classLoader) {
    this.originalClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader);
  }

  @Override
  public void close() {
    Thread.currentThread().setContextClassLoader(originalClassLoader);
  }
}
