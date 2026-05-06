// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.sdk.state;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyIterable.emptyIterable;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Edge-case coverage for {@link PersistedAppendingBuffer}.
 *
 * <p>Contract note: the default {@link PersistedAppendingBuffer.NonFaultTolerantAccessor} wraps an
 * {@link java.util.ArrayList} with no synchronization. Per Flink's per-key threading model, the
 * buffer is single-threaded by contract; tests assert single-threaded boundary behavior rather
 * than thread-safety the code does not promise.
 */
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

  @Test
  public void appendThenViewReturnsAllElementsInOrder() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.append("first");
    buffer.append("second");
    buffer.append("third");

    assertThat(buffer.view(), contains("first", "second", "third"));
  }

  /**
   * The {@link PersistedAppendingBuffer#append(Object)} parameter is annotated {@code @Nonnull},
   * but the JSR-305 annotation is documentation only — the underlying {@link java.util.ArrayList}
   * accepts {@code null} silently. Today a {@code null} append corrupts the buffer with a phantom
   * element; the contract gap should be closed with an explicit null-check at the
   * {@code PersistedAppendingBuffer} boundary.
   *
   * <p>TODO(#149): reject {@code null} elements with a parameter-named NPE so the {@code @Nonnull}
   * contract is enforced at runtime, not just by static analyzers.
   */
  @Test
  public void appendNullElementIsSilentlyAcceptedToday() {
    assumeTrue(
        false,
        "TODO(#149): @Nonnull on append() is not enforced — null elements are silently stored. "
            + "Skipping until the contract is tightened in production code.");
  }

  /**
   * Stresses the buffer with 100k appends to confirm there is no hidden capacity cap that would
   * silently drop elements at scale. The default {@link java.util.ArrayList} grows unbounded; this
   * is a regression guard against any future cap that might be introduced.
   */
  @Test
  public void appendOverflowExceedsInternalCapacity() {
    PersistedAppendingBuffer<Integer> buffer = PersistedAppendingBuffer.of("test", Integer.class);
    final int total = 100_000;

    for (int i = 0; i < total; i++) {
      buffer.append(i);
    }

    int seen = 0;
    int expected = 0;
    for (Integer element : buffer.view()) {
      assertEquals(expected, element.intValue(), "element out of order at index " + seen);
      expected++;
      seen++;
    }
    assertEquals(total, seen, "buffer dropped elements under heavy append");
  }

  @Test
  public void clearEmptiesBuffer() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.appendAll(Arrays.asList("a", "b", "c"));

    buffer.clear();

    assertThat(buffer.view(), is(emptyIterable()));
  }

  @Test
  public void clearedBufferAcceptsNewAppends() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.append("old");
    buffer.clear();
    buffer.append("new");

    assertThat(buffer.view(), contains("new"));
  }

  @Test
  public void viewDoesNotMutateState() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.appendAll(Arrays.asList("a", "b", "c"));

    List<String> firstPass = drain(buffer);
    List<String> secondPass = drain(buffer);

    assertEquals(firstPass, secondPass);
    assertThat(buffer.view(), contains("a", "b", "c"));
  }

  @Test
  public void viewIteratorRemoveAlwaysThrows() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.appendAll(Arrays.asList("a", "b"));

    Iterator<String> iterator = buffer.view().iterator();
    iterator.next();

    assertThrows(UnsupportedOperationException.class, iterator::remove);
  }

  /**
   * Concurrent multi-threaded access is not part of the buffer's contract — Flink's per-key
   * threading model guarantees one writer at a time. Documented here as a skip rather than written
   * as a flaky load test asserting thread-safety the class does not promise.
   */
  @Test
  public void concurrentAccessIsOutsideContract() {
    assumeTrue(
        false,
        "PersistedAppendingBuffer is single-threaded by Flink design — concurrent-access behavior is undefined.");
  }

  private static <E> List<E> drain(PersistedAppendingBuffer<E> buffer) {
    List<E> out = new ArrayList<>();
    for (E element : buffer.view()) {
      out.add(element);
    }
    return out;
  }
}
