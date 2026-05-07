// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.state.ListState;
import org.junit.jupiter.api.Test;

class FlinkAppendingBufferAccessorTest {

  @Test
  void appendDelegatesToListStateAdd() throws Exception {
    FakeListState<String> state = new FakeListState<>();
    FlinkAppendingBufferAccessor<String> accessor = new FlinkAppendingBufferAccessor<>(state);

    accessor.append("a");
    accessor.append("b");

    assertThat(state.backing).containsExactly("a", "b");
  }

  @Test
  void appendAllDelegatesToListStateAddAll() throws Exception {
    FakeListState<String> state = new FakeListState<>();
    FlinkAppendingBufferAccessor<String> accessor = new FlinkAppendingBufferAccessor<>(state);

    accessor.appendAll(Arrays.asList("x", "y", "z"));

    assertThat(state.backing).containsExactly("x", "y", "z");
  }

  @Test
  void replaceWithOverwritesListStateContents() throws Exception {
    FakeListState<String> state = new FakeListState<>();
    state.backing.add("old");
    FlinkAppendingBufferAccessor<String> accessor = new FlinkAppendingBufferAccessor<>(state);

    accessor.replaceWith(Arrays.asList("new1", "new2"));

    assertThat(state.backing).containsExactly("new1", "new2");
  }

  @Test
  void viewReturnsListStateGet() throws Exception {
    FakeListState<String> state = new FakeListState<>();
    state.backing.add("a");
    state.backing.add("b");
    FlinkAppendingBufferAccessor<String> accessor = new FlinkAppendingBufferAccessor<>(state);

    Iterable<String> view = accessor.view();

    assertThat(view).containsExactly("a", "b");
  }

  @Test
  void clearDelegatesToListState() {
    FakeListState<String> state = new FakeListState<>();
    state.backing.add("a");
    FlinkAppendingBufferAccessor<String> accessor = new FlinkAppendingBufferAccessor<>(state);

    accessor.clear();

    assertThat(state.backing).isEmpty();
  }

  @Test
  void wrapsCheckedExceptionsFromMutators() {
    FakeListState<String> state =
        new FakeListState<String>() {
          @Override
          public void add(String value) throws Exception {
            throw new IOException("add-boom");
          }

          @Override
          public void addAll(List<String> values) throws Exception {
            throw new IOException("addAll-boom");
          }

          @Override
          public void update(List<String> values) throws Exception {
            throw new IOException("update-boom");
          }
        };
    FlinkAppendingBufferAccessor<String> accessor = new FlinkAppendingBufferAccessor<>(state);

    assertThatThrownBy(() -> accessor.append("x"))
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(IOException.class);
    assertThatThrownBy(() -> accessor.appendAll(Arrays.asList("a")))
        .isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> accessor.replaceWith(Arrays.asList("a")))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  void wrapsCheckedExceptionsFromView() {
    FakeListState<String> state =
        new FakeListState<String>() {
          @Override
          public Iterable<String> get() throws Exception {
            throw new IOException("get-boom");
          }
        };
    FlinkAppendingBufferAccessor<String> accessor = new FlinkAppendingBufferAccessor<>(state);

    assertThatThrownBy(accessor::view).isInstanceOf(RuntimeException.class);
  }

  @Test
  void wrapsCheckedExceptionFromClear() {
    FakeListState<String> state =
        new FakeListState<String>() {
          @Override
          public void clear() {
            throw new RuntimeException(new IOException("clear-boom"));
          }
        };
    FlinkAppendingBufferAccessor<String> accessor = new FlinkAppendingBufferAccessor<>(state);

    assertThatThrownBy(accessor::clear).isInstanceOf(RuntimeException.class);
  }

  @Test
  void rejectsNullStateHandle() {
    assertThatThrownBy(() -> new FlinkAppendingBufferAccessor<>(null))
        .isInstanceOf(NullPointerException.class);
  }

  /** In-memory ListState used to record FlinkAppendingBufferAccessor's delegation. */
  static class FakeListState<E> implements ListState<E> {
    final List<E> backing = new ArrayList<>();

    @Override
    public Iterable<E> get() throws Exception {
      return new ArrayList<>(backing);
    }

    @Override
    public void add(E value) throws Exception {
      backing.add(value);
    }

    @Override
    public void update(List<E> values) throws Exception {
      backing.clear();
      backing.addAll(values);
    }

    @Override
    public void addAll(List<E> values) throws Exception {
      backing.addAll(values);
    }

    @Override
    public void clear() {
      backing.clear();
    }
  }
}
