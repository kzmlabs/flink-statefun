// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.MapState;
import org.junit.jupiter.api.Test;

class FlinkTableAccessorTest {

  @Test
  void setDelegatesToMapStatePut() throws Exception {
    FakeMapState<String, Integer> state = new FakeMapState<>();
    FlinkTableAccessor<String, Integer> accessor = new FlinkTableAccessor<>(state);

    accessor.set("a", 1);

    assertThat(state.backing).containsEntry("a", 1);
  }

  @Test
  void getDelegatesToMapStateGet() throws Exception {
    FakeMapState<String, Integer> state = new FakeMapState<>();
    state.backing.put("a", 7);
    FlinkTableAccessor<String, Integer> accessor = new FlinkTableAccessor<>(state);

    assertThat(accessor.get("a")).isEqualTo(7);
    assertThat(accessor.get("missing")).isNull();
  }

  @Test
  void removeDelegatesToMapStateRemove() throws Exception {
    FakeMapState<String, Integer> state = new FakeMapState<>();
    state.backing.put("a", 1);
    state.backing.put("b", 2);
    FlinkTableAccessor<String, Integer> accessor = new FlinkTableAccessor<>(state);

    accessor.remove("a");

    assertThat(state.backing).doesNotContainKey("a").containsEntry("b", 2);
  }

  @Test
  void entriesKeysValuesDelegateToMapState() throws Exception {
    FakeMapState<String, Integer> state = new FakeMapState<>();
    state.backing.put("a", 1);
    state.backing.put("b", 2);
    FlinkTableAccessor<String, Integer> accessor = new FlinkTableAccessor<>(state);

    assertThat(toSet(accessor.keys())).containsExactlyInAnyOrder("a", "b");
    assertThat(toSet(accessor.values())).containsExactlyInAnyOrder(1, 2);
    Map<String, Integer> seenEntries = new HashMap<>();
    for (Map.Entry<String, Integer> e : accessor.entries()) {
      seenEntries.put(e.getKey(), e.getValue());
    }
    assertThat(seenEntries).containsEntry("a", 1).containsEntry("b", 2);
  }

  @Test
  void clearDelegatesToMapState() {
    FakeMapState<String, Integer> state = new FakeMapState<>();
    state.backing.put("a", 1);
    FlinkTableAccessor<String, Integer> accessor = new FlinkTableAccessor<>(state);

    accessor.clear();

    assertThat(state.backing).isEmpty();
    assertThat(state.cleared).isTrue();
  }

  @Test
  void wrapsCheckedExceptionFromGetAsRuntime() {
    FakeMapState<String, Integer> state =
        new FakeMapState<String, Integer>() {
          @Override
          public Integer get(String key) throws Exception {
            throw new IOException("boom");
          }
        };
    FlinkTableAccessor<String, Integer> accessor = new FlinkTableAccessor<>(state);

    assertThatThrownBy(() -> accessor.get("a"))
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(IOException.class);
  }

  @Test
  void wrapsCheckedExceptionsFromMutators() {
    FakeMapState<String, Integer> state =
        new FakeMapState<String, Integer>() {
          @Override
          public void put(String k, Integer v) throws Exception {
            throw new IOException("set-boom");
          }

          @Override
          public void remove(String k) throws Exception {
            throw new IOException("remove-boom");
          }
        };
    FlinkTableAccessor<String, Integer> accessor = new FlinkTableAccessor<>(state);

    assertThatThrownBy(() -> accessor.set("a", 1)).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> accessor.remove("a")).isInstanceOf(RuntimeException.class);
  }

  @Test
  void wrapsCheckedExceptionsFromIterators() {
    FakeMapState<String, Integer> state =
        new FakeMapState<String, Integer>() {
          @Override
          public Iterable<Map.Entry<String, Integer>> entries() throws Exception {
            throw new IOException("entries-boom");
          }

          @Override
          public Iterable<String> keys() throws Exception {
            throw new IOException("keys-boom");
          }

          @Override
          public Iterable<Integer> values() throws Exception {
            throw new IOException("values-boom");
          }
        };
    FlinkTableAccessor<String, Integer> accessor = new FlinkTableAccessor<>(state);

    assertThatThrownBy(accessor::entries).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(accessor::keys).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(accessor::values).isInstanceOf(RuntimeException.class);
  }

  @Test
  void rejectsNullStateHandle() {
    assertThatThrownBy(() -> new FlinkTableAccessor<>(null))
        .isInstanceOf(NullPointerException.class);
  }

  private static <T> Set<T> toSet(Iterable<T> it) {
    return StreamSupport.stream(it.spliterator(), false).collect(java.util.stream.Collectors.toSet());
  }

  /**
   * In-memory MapState used to record FlinkTableAccessor's delegation. Most overrides default
   * to "happy-path" delegate behavior; tests subclass this to inject failures.
   */
  static class FakeMapState<K, V> implements MapState<K, V> {
    final Map<K, V> backing = new HashMap<>();
    boolean cleared;

    @Override
    public V get(K key) throws Exception {
      return backing.get(key);
    }

    @Override
    public void put(K key, V value) throws Exception {
      backing.put(key, value);
    }

    @Override
    public void putAll(Map<K, V> map) throws Exception {
      backing.putAll(map);
    }

    @Override
    public void remove(K key) throws Exception {
      backing.remove(key);
    }

    @Override
    public boolean contains(K key) throws Exception {
      return backing.containsKey(key);
    }

    @Override
    public Iterable<Map.Entry<K, V>> entries() throws Exception {
      return backing.entrySet();
    }

    @Override
    public Iterable<K> keys() throws Exception {
      return backing.keySet();
    }

    @Override
    public Iterable<V> values() throws Exception {
      return backing.values();
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() throws Exception {
      return backing.entrySet().iterator();
    }

    @Override
    public boolean isEmpty() throws Exception {
      return backing.isEmpty();
    }

    @Override
    public void clear() {
      backing.clear();
      cleared = true;
    }
  }

}
