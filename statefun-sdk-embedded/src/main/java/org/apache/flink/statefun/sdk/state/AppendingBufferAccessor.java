// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.state;

import java.util.List;
import javax.annotation.Nonnull;

public interface AppendingBufferAccessor<E> {

  void append(@Nonnull E element);

  void appendAll(@Nonnull List<E> elements);

  void replaceWith(@Nonnull List<E> elements);

  @Nonnull
  Iterable<E> view();

  void clear();
}
