// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.sdk.state;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

public class PersistedAppendingBufferTest {

  @Test
  public void viewOnInit() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    assertFalse(buffer.view().iterator().hasNext());
  }

  @Test
  public void append() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.append("element");

    assertThat(buffer.view().iterator().next(), is("element"));
  }

  @Test
  public void appendAll() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.appendAll(Arrays.asList("element-1", "element-2"));

    assertThat(buffer.view(), contains("element-1", "element-2"));
  }

  @Test
  public void appendAllEmptyList() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.append("element");
    buffer.appendAll(new ArrayList<>());

    assertThat(buffer.view().iterator().next(), is("element"));
  }

  @Test
  public void replaceWith() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.append("element");
    buffer.replaceWith(Collections.singletonList("element-new"));

    assertThat(buffer.view().iterator().next(), is("element-new"));
  }

  @Test
  public void replaceWithEmptyList() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.append("element");
    buffer.replaceWith(new ArrayList<>());

    assertFalse(buffer.view().iterator().hasNext());
  }

  @Test
  public void viewAfterClear() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.append("element");
    buffer.clear();

    assertFalse(buffer.view().iterator().hasNext());
  }

  @Test
  public void viewUnmodifiable() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          PersistedAppendingBuffer<String> buffer =
              PersistedAppendingBuffer.of("test", String.class);
          buffer.append("element");

          Iterator<String> view = buffer.view().iterator();
          view.remove();
        });
  }
}
